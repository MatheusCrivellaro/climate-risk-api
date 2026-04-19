"""Caso de uso :class:`ConsultarResultados` (Slice 11).

Consulta rica por :class:`ResultadoIndice` com filtros diversos. O caso
de uso aceita ``raio_km`` em torno de ``(centro_lat, centro_lon)`` e o
converte em BBOX (pré-filtro barato em SQL) antes de delegar ao
repositório, filtrando depois os candidatos pelo raio exato via
Haversine em Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.excecoes import ErroValidacao
from climate_risk.domain.portas.filtros_resultados import FiltrosConsultaResultados
from climate_risk.domain.portas.repositorios import RepositorioResultados
from climate_risk.domain.util.distancia import (
    bbox_aproximado_por_raio,
    distancia_haversine_km,
)

__all__ = ["ConsultarResultados", "FiltrosResultados", "PaginaResultados"]


@dataclass(frozen=True)
class FiltrosResultados:
    """Entrada do caso de uso — espelha a querystring do endpoint.

    Attributes:
        execucao_id: Filtra por execução.
        cenario: Cenário da execução (``rcp45``, ``rcp85``).
        variavel: Variável climática (``pr``, ``tas``).
        ano: Ano exato.
        ano_min/ano_max: Intervalo inclusivo.
        nomes_indices: Lista de nomes de índice para ``IN(...)``.
        lat_min/lat_max/lon_min/lon_max: BBOX direto. ``lon_min`` e
            ``lon_max`` fora de ``[-180, 180]`` indicam cruzamento do
            antimeridiano (ex.: ``lon_min=170, lon_max=200`` equivale a
            ``[170,180] U [-180,-160]``).
        raio_km/centro_lat/centro_lon: Alternativa geográfica ao BBOX —
            os três campos são mutuamente obrigatórios. Quando fornecidos,
            ``BBOX`` explícito é ignorado (o caso de uso computa um BBOX
            aproximado e usa Haversine exato no pós-filtro).
        uf: Filtra via JOIN com municípios.
        municipio_id: Filtro direto.
        limit: Até 1000.
        offset: Paginação.
    """

    execucao_id: str | None = None
    cenario: str | None = None
    variavel: str | None = None
    ano: int | None = None
    ano_min: int | None = None
    ano_max: int | None = None
    nomes_indices: tuple[str, ...] = ()
    lat_min: float | None = None
    lat_max: float | None = None
    lon_min: float | None = None
    lon_max: float | None = None
    raio_km: float | None = None
    centro_lat: float | None = None
    centro_lon: float | None = None
    uf: str | None = None
    municipio_id: int | None = None
    limit: int = 100
    offset: int = 0


@dataclass(frozen=True)
class PaginaResultados:
    """Retorno paginado de :meth:`ConsultarResultados.executar`."""

    total: int
    limit: int
    offset: int
    items: list[ResultadoIndice] = field(default_factory=list)


class ConsultarResultados:
    """Aplica filtros, expande ``raio_km`` em BBOX e pagina.

    Regras:

    - ``raio_km`` exige ``centro_lat`` e ``centro_lon`` (validação
      cruzada, 422).
    - Quando ``raio_km`` é fornecido, o BBOX aproximado é calculado e o
      Haversine exato filtra os candidatos em Python — ordem de ``total``
      e ``items`` reflete o **pós-filtro** (total = linhas de fato dentro
      do raio até ``limit``, com overhead proporcional ao ``limit``).
    - ``limit`` superior a 1000 é rejeitado.
    """

    LIMIT_MAXIMO = 1000

    def __init__(self, repositorio: RepositorioResultados) -> None:
        self._repositorio = repositorio

    async def executar(self, filtros: FiltrosResultados) -> PaginaResultados:
        self._validar(filtros)

        if filtros.raio_km is not None:
            return await self._consultar_com_raio(filtros)

        filtros_repo = self._montar_filtros_repo(filtros)
        items = await self._repositorio.consultar(
            filtros_repo, limit=filtros.limit, offset=filtros.offset
        )
        total = await self._repositorio.contar_por_filtros(filtros_repo)
        return PaginaResultados(
            total=total, limit=filtros.limit, offset=filtros.offset, items=items
        )

    def _validar(self, filtros: FiltrosResultados) -> None:
        if filtros.limit <= 0 or filtros.limit > self.LIMIT_MAXIMO:
            raise ErroValidacao(
                f"'limit' deve estar em (0, {self.LIMIT_MAXIMO}]; recebido {filtros.limit}."
            )
        if filtros.offset < 0:
            raise ErroValidacao(f"'offset' deve ser ≥ 0; recebido {filtros.offset}.")
        raio_campos = (filtros.raio_km, filtros.centro_lat, filtros.centro_lon)
        fornecidos = sum(1 for c in raio_campos if c is not None)
        if 0 < fornecidos < 3:
            raise ErroValidacao(
                "'raio_km', 'centro_lat' e 'centro_lon' devem ser fornecidos juntos."
            )
        if filtros.raio_km is not None and filtros.raio_km <= 0:
            raise ErroValidacao("'raio_km' deve ser maior que zero.")
        if filtros.ano is not None and (filtros.ano_min is not None or filtros.ano_max is not None):
            raise ErroValidacao("'ano' é mutuamente exclusivo com 'ano_min'/'ano_max'.")
        if (
            filtros.ano_min is not None
            and filtros.ano_max is not None
            and filtros.ano_min > filtros.ano_max
        ):
            raise ErroValidacao("'ano_min' não pode ser maior que 'ano_max'.")

    @staticmethod
    def _montar_filtros_repo(filtros: FiltrosResultados) -> FiltrosConsultaResultados:
        return FiltrosConsultaResultados(
            execucao_id=filtros.execucao_id,
            cenario=filtros.cenario,
            variavel=filtros.variavel,
            ano=filtros.ano,
            ano_min=filtros.ano_min,
            ano_max=filtros.ano_max,
            nomes_indices=filtros.nomes_indices,
            lat_min=filtros.lat_min,
            lat_max=filtros.lat_max,
            lon_min=filtros.lon_min,
            lon_max=filtros.lon_max,
            uf=filtros.uf,
            municipio_id=filtros.municipio_id,
        )

    async def _consultar_com_raio(self, filtros: FiltrosResultados) -> PaginaResultados:
        assert filtros.raio_km is not None
        assert filtros.centro_lat is not None
        assert filtros.centro_lon is not None
        lat_min, lat_max, lon_min, lon_max = bbox_aproximado_por_raio(
            filtros.centro_lat, filtros.centro_lon, filtros.raio_km
        )
        base = FiltrosConsultaResultados(
            execucao_id=filtros.execucao_id,
            cenario=filtros.cenario,
            variavel=filtros.variavel,
            ano=filtros.ano,
            ano_min=filtros.ano_min,
            ano_max=filtros.ano_max,
            nomes_indices=filtros.nomes_indices,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            uf=filtros.uf,
            municipio_id=filtros.municipio_id,
        )

        # Buffer grande: pega até LIMIT_MAXIMO candidatos do BBOX para
        # poder filtrar pelo raio exato. O repositório já faz o corte em SQL.
        candidatos = await self._repositorio.consultar(
            base, limit=self.LIMIT_MAXIMO, offset=0
        )
        filtrados = [
            r
            for r in candidatos
            if distancia_haversine_km(
                filtros.centro_lat, filtros.centro_lon, r.lat, r.lon
            )
            <= filtros.raio_km
        ]
        total = len(filtrados)
        fim = filtros.offset + filtros.limit
        items = filtrados[filtros.offset : fim]
        return PaginaResultados(
            total=total, limit=filtros.limit, offset=filtros.offset, items=items
        )
