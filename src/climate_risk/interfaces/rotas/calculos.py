"""Rotas de cálculo de índices (``/calculos``)."""

from __future__ import annotations

import math
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from climate_risk.application.calculos.calcular_por_pontos import (
    CalcularIndicesPorPontos,
    ParametrosCalculo,
    PontoEntradaDominio,
    ResultadoCalculo,
)
from climate_risk.application.calculos.criar_execucao_por_pontos import (
    CriarExecucaoPorPontos,
    ParametrosCriacaoExecucaoPontos,
    ResultadoCriacaoExecucaoPontos,
)
from climate_risk.core.config import Settings
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_calcular_por_pontos,
    obter_caso_uso_criar_execucao_por_pontos,
    obter_settings,
)
from climate_risk.interfaces.schemas.calculos import (
    CalculoPontosAsyncResponse,
    CalculoPorPontosRequest,
    CalculoPorPontosResponse,
    IndicesResposta,
    PontoResultado,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails

router = APIRouter(prefix="/calculos", tags=["calculos"])


SincronoDep = Annotated[CalcularIndicesPorPontos, Depends(obter_caso_uso_calcular_por_pontos)]
AsyncDep = Annotated[CriarExecucaoPorPontos, Depends(obter_caso_uso_criar_execucao_por_pontos)]
SettingsDep = Annotated[Settings, Depends(obter_settings)]


@router.post(
    "/pontos",
    response_model=None,
    summary="Calcula índices anuais para pontos (UC-03 síncrono/assíncrono).",
    responses={
        200: {
            "model": CalculoPorPontosResponse,
            "description": "Lote pequeno processado de forma síncrona.",
        },
        202: {
            "model": CalculoPontosAsyncResponse,
            "description": (
                "Lote grande (> ``sincrono_pontos_max`` pontos): job enfileirado. "
                "Acompanhe via ``GET /execucoes/{id}``."
            ),
        },
        404: {"model": ProblemDetails, "description": "Arquivo NetCDF não encontrado."},
        422: {"model": ProblemDetails, "description": "Erro de validação ou dataset inválido."},
        500: {"model": ProblemDetails, "description": "Erro interno."},
    },
)
async def calcular_por_pontos(
    payload: CalculoPorPontosRequest,
    caso_sincrono: SincronoDep,
    caso_async: AsyncDep,
    settings: SettingsDep,
    response: Response,
) -> CalculoPorPontosResponse | CalculoPontosAsyncResponse:
    """Roteamento síncrono vs. assíncrono.

    - ``len(pontos) <= settings.sincrono_pontos_max`` → 200 com resultados.
    - Caso contrário → 202 com ``execucao_id``/``job_id`` (Slice 7).

    Ambos os ramos traduzem o payload Pydantic para dataclasses de
    ``application`` antes de invocar o caso de uso (ADR-005).
    """
    if len(payload.pontos) > settings.sincrono_pontos_max:
        params_async = _traduzir_request_para_params_async(payload)
        criacao = await caso_async.executar(params_async)
        response.status_code = status.HTTP_202_ACCEPTED
        return _traduzir_criacao_async_para_response(criacao)

    parametros = _traduzir_request_para_params(payload)
    resultado = await caso_sincrono.executar(parametros)
    return _traduzir_resultado_para_response(resultado, total_pontos=len(payload.pontos))


def _traduzir_request_para_params(payload: CalculoPorPontosRequest) -> ParametrosCalculo:
    """Pydantic → dataclasses de domínio/aplicação (sem vazar Pydantic)."""
    parametros_indices = _traduzir_parametros_indices(payload)
    baseline = _traduzir_baseline(payload)
    pontos = _traduzir_pontos(payload)
    return ParametrosCalculo(
        arquivo_nc=payload.arquivo_nc,
        cenario=payload.cenario,
        variavel=payload.variavel,
        pontos=pontos,
        parametros_indices=parametros_indices,
        p95_baseline=baseline,
        p95_wet_thr=payload.parametros_indices.p95_wet_thr,
    )


def _traduzir_request_para_params_async(
    payload: CalculoPorPontosRequest,
) -> ParametrosCriacaoExecucaoPontos:
    """Versão assíncrona — o worker sempre persiste via ProcessarPontosLote."""
    return ParametrosCriacaoExecucaoPontos(
        arquivo_nc=payload.arquivo_nc,
        cenario=payload.cenario,
        variavel=payload.variavel,
        pontos=_traduzir_pontos(payload),
        parametros_indices=_traduzir_parametros_indices(payload),
        p95_baseline=_traduzir_baseline(payload),
        p95_wet_thr=payload.parametros_indices.p95_wet_thr,
    )


def _traduzir_parametros_indices(payload: CalculoPorPontosRequest) -> ParametrosIndices:
    return ParametrosIndices(
        freq_thr_mm=payload.parametros_indices.freq_thr_mm,
        heavy_thresholds=(payload.parametros_indices.heavy20, payload.parametros_indices.heavy50),
    )


def _traduzir_baseline(payload: CalculoPorPontosRequest) -> PeriodoBaseline | None:
    baseline_entrada = payload.parametros_indices.p95_baseline
    if baseline_entrada is None:
        return None
    return PeriodoBaseline(inicio=baseline_entrada.inicio, fim=baseline_entrada.fim)


def _traduzir_pontos(payload: CalculoPorPontosRequest) -> list[PontoEntradaDominio]:
    return [
        PontoEntradaDominio(lat=p.lat, lon=p.lon, identificador=p.identificador)
        for p in payload.pontos
    ]


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
        cenario=resultado.cenario,
        variavel=resultado.variavel,
        total_pontos=total_pontos,
        total_resultados=len(linhas),
        resultados=linhas,
    )


def _traduzir_criacao_async_para_response(
    resultado: ResultadoCriacaoExecucaoPontos,
) -> CalculoPontosAsyncResponse:
    return CalculoPontosAsyncResponse(
        execucao_id=resultado.execucao_id,
        job_id=resultado.job_id,
        status=resultado.status,
        total_pontos=resultado.total_pontos,
        criado_em=resultado.criado_em.isoformat(),
        links={
            "self": f"/execucoes/{resultado.execucao_id}",
            "job": f"/jobs/{resultado.job_id}",
        },
    )


def _finito_ou_none(valor: float) -> float | None:
    """JSON não tem ``NaN``; traduzimos para ``null`` na resposta."""
    return None if math.isnan(valor) else float(valor)
