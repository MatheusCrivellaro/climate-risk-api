"""Caso de uso :class:`CalcularIndicesPorPontos` (UC-03 síncrono).

Orquestra a leitura do NetCDF, o cálculo de P95 por célula, a amostragem da
série diária em cada ponto e o cálculo dos índices anuais. É um caso de uso
**puro**: apenas calcula e devolve :class:`ResultadoCalculo`; não cria
:class:`~climate_risk.domain.entidades.execucao.Execucao` nem persiste
:class:`~climate_risk.domain.entidades.resultado.ResultadoIndice`.

Quem quiser persistir (o fluxo assíncrono em
:mod:`climate_risk.application.calculos.processar_pontos_lote` ou a rota
síncrona, no futuro) deve orquestrar os repositórios na camada chamadora.

ADR-005: esta camada depende **apenas** de ``domain``. Nenhum import de
FastAPI, Pydantic, SQLAlchemy ou ``xarray`` deve aparecer neste módulo.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from climate_risk.domain.espacial.grade import indice_mais_proximo
from climate_risk.domain.indices.calculadora import (
    IndicesAnuais,
    ParametrosIndices,
    calcular_indices_anuais,
)
from climate_risk.domain.indices.p95 import PeriodoBaseline, calcular_p95_por_celula_numpy
from climate_risk.domain.portas.leitor_netcdf import LeitorNetCDF

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
        cenario: Rótulo do cenário (ex.: ``"rcp45"``). O leitor infere o
            cenário real do arquivo; este valor serve de fallback quando
            o leitor devolve ``"unknown"``.
        variavel: Nome da variável climática (MVP: ``"pr"``).
        pontos: Lista de pontos a avaliar. Cardinalidade deve ter sido
            validada pela camada HTTP (limite síncrono).
        parametros_indices: Parâmetros dos índices anuais.
        p95_baseline: Intervalo fechado de anos para o P95; ``None``
            desativa o cálculo do P95.
        p95_wet_thr: Limiar de "dia chuvoso" para o P95 (mm/dia).
    """

    arquivo_nc: str
    cenario: str
    variavel: str
    pontos: Sequence[PontoEntradaDominio]
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float


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

    cenario: str
    variavel: str
    resultados: list[ResultadoPonto]


class CalcularIndicesPorPontos:
    """Orquestrador do UC-03 síncrono.

    Depende apenas de :class:`LeitorNetCDF` (porta de ``domain``). Não
    toca repositórios: a responsabilidade de persistir pertence a quem
    invoca o caso de uso.
    """

    def __init__(self, leitor_netcdf: LeitorNetCDF) -> None:
        self._leitor = leitor_netcdf

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

        return ResultadoCalculo(
            cenario=dados.cenario if dados.cenario != "unknown" else params.cenario,
            variavel=params.variavel,
            resultados=resultados,
        )


def _p95_para_pixel(p95_grid: np.ndarray | None, iy: int, ix: int) -> float | None:
    """Extrai o P95 de um pixel, retornando ``None`` quando ausente/NaN."""
    if p95_grid is None:
        return None
    valor = float(p95_grid[iy, ix])
    return valor if math.isfinite(valor) else None
