"""Caso de uso :class:`GeocodificarLocalizacoes` (UC-04 — Slice 8 / Marco M3).

Recebe pares ``(cidade, uf)`` digitados pelo usuário e devolve
``(lat, lon, municipio_id, metodo)`` usando — nesta ordem — cache local,
*match* exato por nome normalizado e *fuzzy match* por ``rapidfuzz``.

Dependências externas permitidas em ``application/``:

- ``rapidfuzz`` (pura, sem I/O) — ADR-005 permite explicitamente.
- Tipos dos protocolos de ``domain/portas/``.

Tudo o resto (HTTP, shapely, SQL) entra via *ports*.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from rapidfuzz import fuzz, process

from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.domain.portas.calculador_centroide import CalculadorCentroide
from climate_risk.domain.portas.cliente_ibge import ClienteIBGE
from climate_risk.domain.portas.repositorios import RepositorioMunicipios
from climate_risk.domain.util.normalizacao import normalizar_nome_municipio

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = 90.0


@dataclass(frozen=True)
class EntradaLocalizacao:
    """Par (cidade, UF) digitado pelo usuário — ainda não normalizado."""

    cidade: str
    uf: str


@dataclass(frozen=True)
class LocalizacaoGeocodificada:
    """Resultado da geocodificação para uma entrada.

    ``metodo`` identifica o caminho que resolveu a entrada:

    - ``"cache_exato"``: *match* exato por nome normalizado no cache local.
    - ``"cache_fuzzy"``: *match* fuzzy (rapidfuzz) acima de ``FUZZY_THRESHOLD``.
    - ``"ibge"``: precisou consultar a API do IBGE (malha) para obter o
      centroide e gravou no cache.
    - ``"nao_encontrado"``: nenhuma correspondência — ``lat``/``lon``/``id`` ``None``.
    - ``"api_falhou"``: a API do IBGE falhou durante um ``refresh`` necessário;
      devolvido sem bloquear o lote inteiro (degradação graciosa).
    """

    cidade_entrada: str
    uf: str
    municipio_id: int | None
    nome_canonico: str | None
    lat: float | None
    lon: float | None
    metodo: str


@dataclass(frozen=True)
class ResultadoGeocodificacao:
    """Sumário do lote geocodificado."""

    total: int
    encontrados: int
    nao_encontrados: int
    itens: list[LocalizacaoGeocodificada]


class GeocodificarLocalizacoes:
    """Orquestra cache → fuzzy → API com degradação graciosa."""

    def __init__(
        self,
        repositorio_municipios: RepositorioMunicipios,
        cliente_ibge: ClienteIBGE,
        calculador_centroide: CalculadorCentroide,
    ) -> None:
        self._repo = repositorio_municipios
        self._cliente = cliente_ibge
        self._centroide = calculador_centroide

    async def executar(self, entradas: list[EntradaLocalizacao]) -> ResultadoGeocodificacao:
        """Geocodifica um lote completo.

        Cada UF carrega seus municípios do cache uma única vez. Se o cache
        estiver vazio para uma UF, chama :meth:`_sincronizar_uf` — que pode
        falhar com :class:`ErroClienteIBGE`; neste caso todas as entradas
        daquela UF ficam com ``metodo="api_falhou"`` (os demais UFs
        continuam processando normalmente).
        """
        candidatos_por_uf: dict[str, list[Municipio]] = {}
        ufs_em_falha: set[str] = set()
        itens: list[LocalizacaoGeocodificada] = []

        for entrada in entradas:
            uf = entrada.uf.upper()

            if uf in ufs_em_falha:
                itens.append(self._falha(entrada, uf))
                continue

            if uf not in candidatos_por_uf:
                candidatos = await self._repo.listar_por_uf(uf)
                if not candidatos:
                    try:
                        candidatos = await self._sincronizar_uf(uf)
                    except ErroClienteIBGE as exc:
                        logger.warning("Falha ao sincronizar UF %s com IBGE: %s", uf, exc)
                        ufs_em_falha.add(uf)
                        itens.append(self._falha(entrada, uf))
                        continue
                candidatos_por_uf[uf] = candidatos

            itens.append(self._resolver(entrada, uf, candidatos_por_uf[uf]))

        encontrados = sum(1 for i in itens if i.municipio_id is not None)
        return ResultadoGeocodificacao(
            total=len(itens),
            encontrados=encontrados,
            nao_encontrados=len(itens) - encontrados,
            itens=itens,
        )

    def _resolver(
        self,
        entrada: EntradaLocalizacao,
        uf: str,
        candidatos: list[Municipio],
    ) -> LocalizacaoGeocodificada:
        alvo = normalizar_nome_municipio(entrada.cidade)
        if not alvo:
            return LocalizacaoGeocodificada(
                cidade_entrada=entrada.cidade,
                uf=uf,
                municipio_id=None,
                nome_canonico=None,
                lat=None,
                lon=None,
                metodo="nao_encontrado",
            )

        # 1) Exato por nome_normalizado.
        exato = next((m for m in candidatos if m.nome_normalizado == alvo), None)
        if exato is not None:
            return self._para_resultado(entrada, uf, exato, "cache_exato")

        # 2) Fuzzy com rapidfuzz (WRatio).
        escolha = process.extractOne(
            alvo,
            {m.id: m.nome_normalizado for m in candidatos},
            scorer=fuzz.WRatio,
            score_cutoff=FUZZY_THRESHOLD,
        )
        if escolha is None:
            return LocalizacaoGeocodificada(
                cidade_entrada=entrada.cidade,
                uf=uf,
                municipio_id=None,
                nome_canonico=None,
                lat=None,
                lon=None,
                metodo="nao_encontrado",
            )
        _, _, municipio_id = escolha
        municipio = next(m for m in candidatos if m.id == municipio_id)
        return self._para_resultado(entrada, uf, municipio, "cache_fuzzy")

    @staticmethod
    def _para_resultado(
        entrada: EntradaLocalizacao,
        uf: str,
        municipio: Municipio,
        metodo: str,
    ) -> LocalizacaoGeocodificada:
        return LocalizacaoGeocodificada(
            cidade_entrada=entrada.cidade,
            uf=uf,
            municipio_id=municipio.id,
            nome_canonico=municipio.nome,
            lat=municipio.lat_centroide,
            lon=municipio.lon_centroide,
            metodo=metodo,
        )

    @staticmethod
    def _falha(entrada: EntradaLocalizacao, uf: str) -> LocalizacaoGeocodificada:
        return LocalizacaoGeocodificada(
            cidade_entrada=entrada.cidade,
            uf=uf,
            municipio_id=None,
            nome_canonico=None,
            lat=None,
            lon=None,
            metodo="api_falhou",
        )

    async def _sincronizar_uf(self, uf: str) -> list[Municipio]:
        """Popula o cache para uma UF ausente — chamada *lazy*.

        Estratégia: baixa o catálogo completo do IBGE (uma única rota),
        filtra a UF, busca a malha de cada município, calcula centroide e
        grava em lote. Caro (~1 req por município), mas só roda uma vez por
        UF por cold start.
        """
        todos = await self._cliente.listar_municipios()
        agora = utc_now()
        novos: list[Municipio] = []
        for m in todos:
            if m.uf != uf:
                continue
            municipio = await self._hidratar_centroide(m.id, m.nome, m.uf, agora)
            novos.append(municipio)
        if novos:
            await self._repo.salvar_lote(novos)
        return novos

    async def _hidratar_centroide(self, id_: int, nome: str, uf: str, agora: datetime) -> Municipio:
        """Consulta a malha e retorna :class:`Municipio` com centroide."""
        try:
            geojson = await self._cliente.obter_geometria_municipio(id_)
            lat, lon = self._centroide.calcular(geojson)
        except ErroClienteIBGE as exc:
            logger.warning(
                "Municipio %s (%s/%s) sem centroide — malha falhou: %s",
                id_,
                nome,
                uf,
                exc,
            )
            lat, lon = None, None
        return Municipio(
            id=id_,
            nome=nome,
            nome_normalizado=normalizar_nome_municipio(nome),
            uf=uf,
            lat_centroide=lat,
            lon_centroide=lon,
            atualizado_em=agora,
        )


@dataclass(frozen=True)
class ResultadoRefreshIBGE:
    """Sumário do ``POST /admin/ibge/refresh``."""

    total_municipios: int
    com_centroide: int
    sem_centroide: int


class RefreshCatalogoIBGE:
    """Repovoa (upsert) todo o catálogo IBGE — executado por ``POST /admin/ibge/refresh``.

    Difere de :meth:`GeocodificarLocalizacoes._sincronizar_uf`:
    refresca TODAS as UFs de uma vez e devolve um sumário numérico para o
    operador — não é chamado no caminho quente.
    """

    def __init__(
        self,
        repositorio_municipios: RepositorioMunicipios,
        cliente_ibge: ClienteIBGE,
        calculador_centroide: CalculadorCentroide,
    ) -> None:
        self._repo = repositorio_municipios
        self._cliente = cliente_ibge
        self._centroide = calculador_centroide

    async def executar(self) -> ResultadoRefreshIBGE:
        agora = utc_now()
        catalogo = await self._cliente.listar_municipios()
        municipios: list[Municipio] = []
        com_centroide = 0
        for m in catalogo:
            try:
                geojson = await self._cliente.obter_geometria_municipio(m.id)
                lat, lon = self._centroide.calcular(geojson)
                com_centroide += 1
            except ErroClienteIBGE as exc:
                logger.warning(
                    "Municipio %s (%s/%s) sem centroide — malha falhou: %s",
                    m.id,
                    m.nome,
                    m.uf,
                    exc,
                )
                lat, lon = None, None
            municipios.append(
                Municipio(
                    id=m.id,
                    nome=m.nome,
                    nome_normalizado=normalizar_nome_municipio(m.nome),
                    uf=m.uf,
                    lat_centroide=lat,
                    lon_centroide=lon,
                    atualizado_em=agora,
                )
            )
        if municipios:
            await self._repo.salvar_lote(municipios)
        return ResultadoRefreshIBGE(
            total_municipios=len(municipios),
            com_centroide=com_centroide,
            sem_centroide=len(municipios) - com_centroide,
        )
