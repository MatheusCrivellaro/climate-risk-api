"""Caso de uso :class:`AgregarResultados` (Slice 11)."""

from __future__ import annotations

from dataclasses import dataclass, field

from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.excecoes import ErroValidacao
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
)
from climate_risk.domain.portas.repositorios import RepositorioResultados
from climate_risk.domain.util.distancia import (
    bbox_aproximado_por_raio,
    distancia_haversine_km,
)

__all__ = [
    "AgregarResultados",
    "FiltrosAgregacao",
    "GrupoAgregado",
    "ResultadoAgregacao",
]


_AGREGACOES = frozenset({"media", "min", "max", "count", "p50", "p95"})
_DIMENSOES = frozenset({"ano", "cenario", "variavel", "nome_indice", "municipio"})


@dataclass(frozen=True)
class FiltrosAgregacao:
    """Entrada do caso de uso de agregação.

    Espelha o endpoint ``GET /resultados/agregados``. Os mesmos filtros de
    ``ConsultarResultados`` podem restringir o universo antes do
    ``GROUP BY``.
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
    agregacao: str = "media"
    agrupar_por: tuple[str, ...] = ()


@dataclass(frozen=True)
class GrupoAgregado:
    """Um bucket do resultado."""

    grupo: dict[str, str | int]
    valor: float | None
    n_amostras: int


@dataclass(frozen=True)
class ResultadoAgregacao:
    """Retorno de :meth:`AgregarResultados.executar`."""

    agregacao: str
    agrupar_por: tuple[str, ...]
    grupos: list[GrupoAgregado] = field(default_factory=list)


class AgregarResultados:
    """Aplica filtros e delega o ``GROUP BY`` ao repositório.

    Quando ``raio_km`` é usado, o caso de uso carrega os resultados do
    BBOX aproximado, filtra pelo raio exato em Python e agrega o
    subconjunto em memória (sem consultar o repositório em SQL). Isso é
    adequado para os volumes atuais (alguns milhares de pontos por
    execução) e mantém a semântica consistente com ``ConsultarResultados``.
    """

    LIMIT_BUFFER_RAIO = 100_000

    def __init__(self, repositorio: RepositorioResultados) -> None:
        self._repositorio = repositorio

    async def executar(self, filtros: FiltrosAgregacao) -> ResultadoAgregacao:
        self._validar(filtros)

        if filtros.raio_km is not None:
            grupos = await self._agregar_em_memoria(filtros)
        else:
            grupos = await self._agregar_no_repositorio(filtros)

        return ResultadoAgregacao(
            agregacao=filtros.agregacao,
            agrupar_por=filtros.agrupar_por,
            grupos=grupos,
        )

    def _validar(self, filtros: FiltrosAgregacao) -> None:
        if filtros.agregacao not in _AGREGACOES:
            raise ErroValidacao(
                f"'agregacao' deve ser um de {sorted(_AGREGACOES)}; recebido '{filtros.agregacao}'."
            )
        for dim in filtros.agrupar_por:
            if dim not in _DIMENSOES:
                raise ErroValidacao(
                    f"'agrupar_por' contém dimensão inválida: '{dim}'. "
                    f"Valores aceitos: {sorted(_DIMENSOES)}."
                )
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

    async def _agregar_no_repositorio(self, filtros: FiltrosAgregacao) -> list[GrupoAgregado]:
        filtros_repo = FiltrosConsultaResultados(
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
        filtros_agg = FiltrosAgregacaoResultados(
            filtros=filtros_repo,
            agregacao=filtros.agregacao,
            agrupar_por=filtros.agrupar_por,
        )
        grupos_raw = await self._repositorio.agregar(filtros_agg)
        return [
            GrupoAgregado(grupo=g.grupo, valor=g.valor, n_amostras=g.n_amostras) for g in grupos_raw
        ]

    async def _agregar_em_memoria(self, filtros: FiltrosAgregacao) -> list[GrupoAgregado]:
        assert filtros.raio_km is not None
        assert filtros.centro_lat is not None
        assert filtros.centro_lon is not None
        lat_min, lat_max, lon_min, lon_max = bbox_aproximado_por_raio(
            filtros.centro_lat, filtros.centro_lon, filtros.raio_km
        )
        filtros_repo = FiltrosConsultaResultados(
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
        candidatos = await self._repositorio.consultar(
            filtros_repo, limit=self.LIMIT_BUFFER_RAIO, offset=0
        )
        filtrados = [
            r
            for r in candidatos
            if distancia_haversine_km(filtros.centro_lat, filtros.centro_lon, r.lat, r.lon)
            <= filtros.raio_km
        ]
        return _agregar_em_python(filtrados, filtros.agregacao, filtros.agrupar_por)


def _agregar_em_python(
    resultados: list[ResultadoIndice], agregacao: str, agrupar_por: tuple[str, ...]
) -> list[GrupoAgregado]:
    """Agregação em memória usada para o caminho ``raio_km``.

    Agrupa em um dict mantendo ordem de primeira aparição da chave.
    Para ``count``, ``n_amostras`` contabiliza todas as linhas (incluindo
    ``valor is None``); para as demais, apenas as não nulas.
    """
    buckets: dict[tuple[object, ...], list[float]] = {}
    totais: dict[tuple[object, ...], int] = {}

    for r in resultados:
        chave = _extrair_chave(r, agrupar_por)
        totais[chave] = totais.get(chave, 0) + 1
        valor = r.valor
        if valor is not None:
            buckets.setdefault(chave, []).append(float(valor))
        else:
            buckets.setdefault(chave, [])

    saida: list[GrupoAgregado] = []
    for chave, valores in buckets.items():
        grupo_dict: dict[str, str | int] = {}
        for dim, valor_chave in zip(agrupar_por, chave, strict=True):
            if valor_chave is None:
                continue
            if dim in ("ano", "municipio") and isinstance(valor_chave, int):
                grupo_dict[dim] = valor_chave
            else:
                grupo_dict[dim] = str(valor_chave)
        total_bucket = totais[chave]
        if agregacao == "count":
            valor_final: float | None = float(total_bucket)
            n_amostras = total_bucket
        else:
            valor_final = _aplicar_agregacao(valores, agregacao)
            n_amostras = len(valores)
        saida.append(GrupoAgregado(grupo=grupo_dict, valor=valor_final, n_amostras=n_amostras))
    return saida


def _extrair_chave(resultado: ResultadoIndice, agrupar_por: tuple[str, ...]) -> tuple[object, ...]:
    chaves: list[object] = []
    for dim in agrupar_por:
        if dim == "ano":
            chaves.append(resultado.ano)
        elif dim == "nome_indice":
            chaves.append(resultado.nome_indice)
        elif dim == "municipio":
            chaves.append(resultado.municipio_id)
        elif dim in ("cenario", "variavel"):
            # Sem JOIN em memória: estas dimensões não são resolvíveis no
            # caminho com raio_km — caem num bucket único com chave None.
            chaves.append(None)
    return tuple(chaves)


def _aplicar_agregacao(valores: list[float], agregacao: str) -> float | None:
    """Aplica ``agregacao`` sobre uma lista de valores não nulos.

    ``"count"`` é tratado fora (no call site), por precisar contar também
    as linhas com ``valor IS NULL``; nunca cai aqui.
    """
    if not valores:
        return None
    if agregacao == "media":
        return sum(valores) / len(valores)
    if agregacao == "min":
        return min(valores)
    if agregacao == "max":
        return max(valores)
    # p50 ou p95 — o validador garante que não há outro valor.
    return _percentil_local(valores, 0.5 if agregacao == "p50" else 0.95)


def _percentil_local(valores: list[float], quantil: float) -> float:
    import statistics

    if len(valores) == 1:
        return valores[0]
    ordenados = sorted(valores)
    cortes = statistics.quantiles(ordenados, n=100, method="inclusive")
    indice = round(quantil * 100) - 1
    return float(cortes[indice])
