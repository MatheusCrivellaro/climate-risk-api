"""Caso de uso :class:`CalcularIndicesPorPontos` (UC-03 síncrono).

Orquestra a leitura do NetCDF, o cálculo de P95 por célula, a amostragem da
série diária em cada ponto, o cálculo dos índices anuais e, opcionalmente, a
persistência de :class:`~climate_risk.domain.entidades.resultado.ResultadoIndice`.

ADR-005: esta camada depende **apenas** de ``domain``. Nenhum import de
FastAPI, Pydantic, SQLAlchemy ou ``xarray`` deve aparecer neste módulo.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.grade import indice_mais_proximo
from climate_risk.domain.indices.calculadora import (
    IndicesAnuais,
    ParametrosIndices,
    calcular_indices_anuais,
)
from climate_risk.domain.indices.p95 import PeriodoBaseline, calcular_p95_por_celula_numpy
from climate_risk.domain.portas.leitor_netcdf import LeitorNetCDF
from climate_risk.domain.portas.repositorios import (
    RepositorioExecucoes,
    RepositorioResultados,
)

__all__ = [
    "UNIDADES_POR_INDICE",
    "CalcularIndicesPorPontos",
    "ParametrosCalculo",
    "PontoEntradaDominio",
    "ResultadoCalculo",
    "ResultadoPonto",
]

# Mapeamento nome_indice -> unidade persistida. Fixo por decisão (ver brief do
# Slice 4): mantém consistência com a especificação do modelo de dados
# (desenho-api.md §5, coluna ``unidade``).
UNIDADES_POR_INDICE: dict[str, str] = {
    "wet_days": "dias",
    "sdii": "mm/day",
    "rx1day": "mm",
    "rx5day": "mm",
    "r20mm": "dias",
    "r50mm": "dias",
    "r95ptot_mm": "mm",
    "r95ptot_frac": "adimensional",
}


@dataclass(frozen=True)
class PontoEntradaDominio:
    """Ponto de entrada do caso de uso — DTO puro.

    O schema Pydantic da camada ``interfaces`` traduz para este dataclass
    antes de invocar o caso de uso, garantindo que ``application`` não
    dependa de Pydantic (ADR-005).
    """

    lat: float
    lon: float
    identificador: str | None


@dataclass(frozen=True)
class ParametrosCalculo:
    """Entrada agregada do caso de uso.

    Atributos:
        arquivo_nc: Caminho do ``.nc`` de origem (passado ao leitor).
        cenario: Rótulo do cenário (ex.: ``"rcp45"``). Usado apenas para
            preencher :class:`Execucao`; o leitor infere o cenário real do
            arquivo.
        variavel: Nome da variável climática (MVP: ``"pr"``).
        pontos: Lista de pontos a avaliar. Cardinalidade deve ter sido
            validada pela camada HTTP (limite síncrono).
        parametros_indices: Parâmetros dos índices anuais.
        p95_baseline: Intervalo fechado de anos para o P95; ``None``
            desativa o cálculo do P95.
        p95_wet_thr: Limiar de "dia chuvoso" para o P95 (mm/dia).
        persistir: Quando ``True``, cria :class:`Execucao` e grava
            :class:`ResultadoIndice` via repositórios.
    """

    arquivo_nc: str
    cenario: str
    variavel: str
    pontos: Sequence[PontoEntradaDominio]
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float
    persistir: bool


@dataclass(frozen=True)
class ResultadoPonto:
    """Índices anuais calculados para um ponto em um ano específico."""

    identificador: str | None
    lat_input: float
    lon_input: float
    lat_grid: float
    lon_grid: float
    ano: int
    indices: IndicesAnuais


@dataclass(frozen=True)
class ResultadoCalculo:
    """Retorno agregado do caso de uso."""

    execucao_id: str | None
    cenario: str
    variavel: str
    resultados: list[ResultadoPonto]


class CalcularIndicesPorPontos:
    """Orquestrador do UC-03 síncrono.

    Dependências (portas de ``domain``):

    - :class:`LeitorNetCDF` para abrir o arquivo e devolver
      :class:`DadosClimaticos`.
    - :class:`RepositorioExecucoes` / :class:`RepositorioResultados` para
      persistência opcional (``persistir=True``).
    """

    def __init__(
        self,
        leitor_netcdf: LeitorNetCDF,
        repositorio_execucoes: RepositorioExecucoes,
        repositorio_resultados: RepositorioResultados,
    ) -> None:
        self._leitor = leitor_netcdf
        self._repo_execucoes = repositorio_execucoes
        self._repo_resultados = repositorio_resultados

    async def executar(self, params: ParametrosCalculo) -> ResultadoCalculo:
        """Executa o fluxo completo. Ver :class:`ParametrosCalculo`."""
        dados = await self._leitor.abrir(params.arquivo_nc, params.variavel)

        p95_grid = calcular_p95_por_celula_numpy(
            dados_diarios=dados.dados_diarios,
            anos_por_dia=dados.anos,
            baseline=params.p95_baseline,
            p95_wet_thr=params.p95_wet_thr,
        )

        anos_unicos = sorted({int(a) for a in np.asarray(dados.anos).tolist()})

        resultados: list[ResultadoPonto] = []
        for ponto in params.pontos:
            iy, ix = indice_mais_proximo(dados.lat_2d, dados.lon_2d, ponto.lat, ponto.lon)
            lat_grid = float(dados.lat_2d[iy, ix])
            lon_grid = float(dados.lon_2d[iy, ix])

            serie_pixel = np.asarray(dados.dados_diarios[:, iy, ix])
            p95_thr = _p95_para_pixel(p95_grid, iy, ix)

            for ano in anos_unicos:
                mascara_ano = dados.anos == ano
                if not bool(np.any(mascara_ano)):
                    continue
                serie_anual = serie_pixel[mascara_ano]
                indices = calcular_indices_anuais(
                    serie_anual, params.parametros_indices, p95_thr=p95_thr
                )
                resultados.append(
                    ResultadoPonto(
                        identificador=ponto.identificador,
                        lat_input=ponto.lat,
                        lon_input=ponto.lon,
                        lat_grid=lat_grid,
                        lon_grid=lon_grid,
                        ano=ano,
                        indices=indices,
                    )
                )

        execucao_id: str | None = None
        if params.persistir:
            execucao_id = await self._persistir(params, resultados)

        return ResultadoCalculo(
            execucao_id=execucao_id,
            cenario=dados.cenario if dados.cenario != "unknown" else params.cenario,
            variavel=params.variavel,
            resultados=resultados,
        )

    async def _persistir(
        self,
        params: ParametrosCalculo,
        resultados: list[ResultadoPonto],
    ) -> str:
        """Cria :class:`Execucao` e grava :class:`ResultadoIndice` em lote."""
        agora = utc_now()
        execucao = Execucao(
            id=gerar_id("exec"),
            cenario=params.cenario,
            variavel=params.variavel,
            arquivo_origem=params.arquivo_nc,
            tipo="pontos",
            parametros={
                "freq_thr_mm": params.parametros_indices.freq_thr_mm,
                "heavy_thresholds": list(params.parametros_indices.heavy_thresholds),
                "p95_wet_thr": params.p95_wet_thr,
                "p95_baseline": (
                    {
                        "inicio": params.p95_baseline.inicio,
                        "fim": params.p95_baseline.fim,
                    }
                    if params.p95_baseline is not None
                    else None
                ),
                "total_pontos": len(params.pontos),
            },
            status=StatusExecucao.COMPLETED,
            criado_em=agora,
            concluido_em=agora,
            job_id=None,
        )
        await self._repo_execucoes.salvar(execucao)

        registros: list[ResultadoIndice] = []
        for ponto in resultados:
            indices_por_nome = _achatar_indices(ponto.indices)
            for nome_indice, valor_bruto in indices_por_nome.items():
                registros.append(
                    ResultadoIndice(
                        id=gerar_id("res"),
                        execucao_id=execucao.id,
                        lat=ponto.lat_grid,
                        lon=ponto.lon_grid,
                        lat_input=ponto.lat_input,
                        lon_input=ponto.lon_input,
                        ano=ponto.ano,
                        nome_indice=nome_indice,
                        valor=_nan_para_none(valor_bruto),
                        unidade=UNIDADES_POR_INDICE[nome_indice],
                        municipio_id=None,
                    )
                )

        if registros:
            await self._repo_resultados.salvar_lote(registros)

        return execucao.id


def _p95_para_pixel(p95_grid: np.ndarray | None, iy: int, ix: int) -> float | None:
    """Extrai o P95 de um pixel, retornando ``None`` quando ausente/NaN."""
    if p95_grid is None:
        return None
    valor = float(p95_grid[iy, ix])
    return valor if math.isfinite(valor) else None


def _achatar_indices(indices: IndicesAnuais) -> dict[str, float]:
    """Converte :class:`IndicesAnuais` em ``{nome_indice: valor}``."""
    return {
        "wet_days": float(indices.wet_days),
        "sdii": indices.sdii,
        "rx1day": indices.rx1day,
        "rx5day": indices.rx5day,
        "r20mm": float(indices.r20mm),
        "r50mm": float(indices.r50mm),
        "r95ptot_mm": indices.r95ptot_mm,
        "r95ptot_frac": indices.r95ptot_frac,
    }


def _nan_para_none(valor: float) -> float | None:
    """Traduz ``NaN`` para ``None`` antes da persistência (coluna REAL nullable)."""
    return None if math.isnan(valor) else float(valor)
