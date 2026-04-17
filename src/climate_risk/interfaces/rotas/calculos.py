"""Rotas de cálculo de índices (``/calculos``)."""

from __future__ import annotations

import math
from typing import Annotated

from fastapi import APIRouter, Depends, status

from climate_risk.application.calculos.calcular_por_pontos import (
    CalcularIndicesPorPontos,
    ParametrosCalculo,
    PontoEntradaDominio,
    ResultadoCalculo,
)
from climate_risk.core.config import Settings
from climate_risk.domain.excecoes import ErroLimitePontosSincrono
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_calcular_por_pontos,
    obter_settings,
)
from climate_risk.interfaces.schemas.calculos import (
    CalculoPorPontosRequest,
    CalculoPorPontosResponse,
    IndicesResposta,
    PontoResultado,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails

router = APIRouter(prefix="/calculos", tags=["calculos"])


CasoUsoDep = Annotated[CalcularIndicesPorPontos, Depends(obter_caso_uso_calcular_por_pontos)]
SettingsDep = Annotated[Settings, Depends(obter_settings)]


@router.post(
    "/pontos",
    response_model=CalculoPorPontosResponse,
    status_code=status.HTTP_200_OK,
    summary="Calcula índices anuais para uma lista de pontos (UC-03 síncrono).",
    responses={
        400: {"model": ProblemDetails, "description": "Violação de regra de negócio."},
        404: {"model": ProblemDetails, "description": "Arquivo NetCDF não encontrado."},
        422: {"model": ProblemDetails, "description": "Erro de validação ou dataset inválido."},
        500: {"model": ProblemDetails, "description": "Erro interno."},
    },
)
async def calcular_por_pontos(
    payload: CalculoPorPontosRequest,
    caso_uso: CasoUsoDep,
    settings: SettingsDep,
) -> CalculoPorPontosResponse:
    """Executa o fluxo UC-03 síncrono end-to-end.

    - Valida o limite de ``settings.sincrono_pontos_max`` pontos (levanta
      :class:`ErroLimitePontosSincrono` → 400).
    - Traduz o payload Pydantic para DTOs de ``application`` (ADR-005).
    - Invoca o caso de uso.
    - Traduz o resultado para a resposta Pydantic.
    """
    if len(payload.pontos) > settings.sincrono_pontos_max:
        raise ErroLimitePontosSincrono(
            total=len(payload.pontos),
            maximo=settings.sincrono_pontos_max,
        )

    parametros = _traduzir_request_para_params(payload)
    resultado = await caso_uso.executar(parametros)
    return _traduzir_resultado_para_response(resultado, total_pontos=len(payload.pontos))


def _traduzir_request_para_params(payload: CalculoPorPontosRequest) -> ParametrosCalculo:
    """Pydantic → dataclasses de domínio/aplicação (sem vazar Pydantic)."""
    parametros_indices = ParametrosIndices(
        freq_thr_mm=payload.parametros_indices.freq_thr_mm,
        heavy_thresholds=(payload.parametros_indices.heavy20, payload.parametros_indices.heavy50),
    )
    baseline: PeriodoBaseline | None = None
    if payload.parametros_indices.p95_baseline is not None:
        baseline = PeriodoBaseline(
            inicio=payload.parametros_indices.p95_baseline.inicio,
            fim=payload.parametros_indices.p95_baseline.fim,
        )
    pontos = [
        PontoEntradaDominio(lat=p.lat, lon=p.lon, identificador=p.identificador)
        for p in payload.pontos
    ]
    return ParametrosCalculo(
        arquivo_nc=payload.arquivo_nc,
        cenario=payload.cenario,
        variavel=payload.variavel,
        pontos=pontos,
        parametros_indices=parametros_indices,
        p95_baseline=baseline,
        p95_wet_thr=payload.parametros_indices.p95_wet_thr,
        persistir=payload.persistir,
    )


def _traduzir_resultado_para_response(
    resultado: ResultadoCalculo,
    total_pontos: int,
) -> CalculoPorPontosResponse:
    """Dataclass de domínio → Pydantic de resposta."""
    linhas = [
        PontoResultado(
            identificador=r.identificador,
            lat_input=r.lat_input,
            lon_input=r.lon_input,
            lat_grid=r.lat_grid,
            lon_grid=r.lon_grid,
            ano=r.ano,
            indices=IndicesResposta(
                wet_days=r.indices.wet_days,
                sdii=_finito_ou_none(r.indices.sdii),
                rx1day=_finito_ou_none(r.indices.rx1day),
                rx5day=_finito_ou_none(r.indices.rx5day),
                r20mm=r.indices.r20mm,
                r50mm=r.indices.r50mm,
                r95ptot_mm=_finito_ou_none(r.indices.r95ptot_mm),
                r95ptot_frac=_finito_ou_none(r.indices.r95ptot_frac),
            ),
        )
        for r in resultado.resultados
    ]
    return CalculoPorPontosResponse(
        execucao_id=resultado.execucao_id,
        cenario=resultado.cenario,
        variavel=resultado.variavel,
        total_pontos=total_pontos,
        total_resultados=len(linhas),
        resultados=linhas,
    )


def _finito_ou_none(valor: float) -> float | None:
    """JSON não tem ``NaN``; traduzimos para ``null`` na resposta."""
    return None if math.isnan(valor) else float(valor)
