"""Rotas REST para execuções CORDEX (``/execucoes``).

Expõe o fluxo assíncrono UC-02:

- ``POST /execucoes``   → cria execução + enfileira job (202).
- ``GET  /execucoes``   → lista com filtros.
- ``GET  /execucoes/{id}`` → detalhe.
- ``POST /execucoes/{id}/cancelar`` → transição ``pending → canceled``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from climate_risk.application.execucoes.cancelar import CancelarExecucao
from climate_risk.application.execucoes.consultar import ConsultarExecucoes
from climate_risk.application.execucoes.criar import (
    CriarExecucaoCordex,
    ParametrosCriacaoExecucao,
    ResultadoCriacaoExecucao,
)
from climate_risk.domain.entidades.execucao import Execucao
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_cancelar_execucao,
    obter_caso_uso_criar_execucao,
    obter_consultar_execucoes,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.execucoes import (
    CriarExecucaoRequest,
    CriarExecucaoResponse,
    ExecucaoResumo,
    ListaExecucoesResponse,
)

router = APIRouter(prefix="/execucoes", tags=["execucoes"])


CriarDep = Annotated[CriarExecucaoCordex, Depends(obter_caso_uso_criar_execucao)]
ConsultarDep = Annotated[ConsultarExecucoes, Depends(obter_consultar_execucoes)]
CancelarDep = Annotated[CancelarExecucao, Depends(obter_caso_uso_cancelar_execucao)]


@router.post(
    "",
    response_model=CriarExecucaoResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Cria uma execução CORDEX e enfileira o job de processamento.",
    responses={
        404: {"model": ProblemDetails, "description": "Arquivo NetCDF não encontrado."},
        422: {"model": ProblemDetails, "description": "Erro de validação do corpo."},
    },
)
async def criar_execucao(
    payload: CriarExecucaoRequest,
    caso_uso: CriarDep,
) -> CriarExecucaoResponse:
    """Enfileira um job CORDEX. Retorna 202 com ``links.self`` e ``links.job``."""
    parametros = _traduzir_request_para_params(payload)
    resultado = await caso_uso.executar(parametros)
    return _traduzir_criacao_para_response(resultado)


@router.get(
    "",
    response_model=ListaExecucoesResponse,
    status_code=status.HTTP_200_OK,
    summary="Lista execuções com filtros opcionais por cenário/variável/status.",
)
async def listar_execucoes(
    caso_uso: ConsultarDep,
    cenario: str | None = Query(default=None),
    variavel: str | None = Query(default=None),
    status_filtro: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> ListaExecucoesResponse:
    resultado = await caso_uso.listar(
        cenario=cenario,
        variavel=variavel,
        status=status_filtro,
        limit=limit,
        offset=offset,
    )
    return ListaExecucoesResponse(
        total=resultado.total,
        limit=resultado.limit,
        offset=resultado.offset,
        items=[_para_resumo(e) for e in resultado.items],
    )


@router.get(
    "/{execucao_id}",
    response_model=ExecucaoResumo,
    status_code=status.HTTP_200_OK,
    summary="Obtém detalhes de uma execução.",
    responses={
        404: {"model": ProblemDetails, "description": "Execução não encontrada."},
    },
)
async def obter_execucao(execucao_id: str, caso_uso: ConsultarDep) -> ExecucaoResumo:
    execucao = await caso_uso.buscar_por_id(execucao_id)
    return _para_resumo(execucao)


@router.post(
    "/{execucao_id}/cancelar",
    response_model=ExecucaoResumo,
    status_code=status.HTTP_200_OK,
    summary="Cancela uma execução em estado 'pending'.",
    responses={
        404: {"model": ProblemDetails, "description": "Execução não encontrada."},
        409: {"model": ProblemDetails, "description": "Estado atual não permite cancelamento."},
    },
)
async def cancelar_execucao(execucao_id: str, caso_uso: CancelarDep) -> ExecucaoResumo:
    execucao = await caso_uso.executar(execucao_id)
    return _para_resumo(execucao)


# ---------------------------------------------------------------------
# Translators Pydantic <-> domain DTOs.
# ---------------------------------------------------------------------
def _traduzir_request_para_params(
    payload: CriarExecucaoRequest,
) -> ParametrosCriacaoExecucao:
    parametros_indices = ParametrosIndices(
        freq_thr_mm=payload.parametros_indices.freq_thr_mm,
        heavy_thresholds=(
            payload.parametros_indices.heavy20,
            payload.parametros_indices.heavy50,
        ),
    )
    baseline = _resolver_baseline(payload)
    bbox = _traduzir_bbox(payload)
    return ParametrosCriacaoExecucao(
        arquivo_nc=payload.arquivo_nc,
        cenario=payload.cenario,
        variavel=payload.variavel,
        bbox=bbox,
        parametros_indices=parametros_indices,
        p95_baseline=baseline,
        p95_wet_thr=payload.parametros_indices.p95_wet_thr,
    )


def _resolver_baseline(payload: CriarExecucaoRequest) -> PeriodoBaseline | None:
    """Precedência: ``p95_baseline`` (top-level) → ``parametros_indices.p95_baseline``."""
    if payload.p95_baseline is not None:
        return PeriodoBaseline(inicio=payload.p95_baseline.inicio, fim=payload.p95_baseline.fim)
    if payload.parametros_indices.p95_baseline is not None:
        return PeriodoBaseline(
            inicio=payload.parametros_indices.p95_baseline.inicio,
            fim=payload.parametros_indices.p95_baseline.fim,
        )
    return None


def _traduzir_bbox(payload: CriarExecucaoRequest) -> BoundingBox | None:
    if payload.bbox is None:
        return None
    return BoundingBox(
        lat_min=payload.bbox.lat_min,
        lat_max=payload.bbox.lat_max,
        lon_min=payload.bbox.lon_min,
        lon_max=payload.bbox.lon_max,
    )


def _traduzir_criacao_para_response(
    resultado: ResultadoCriacaoExecucao,
) -> CriarExecucaoResponse:
    return CriarExecucaoResponse(
        execucao_id=resultado.execucao_id,
        job_id=resultado.job_id,
        status=resultado.status,
        criado_em=resultado.criado_em.isoformat(),
        links={
            "self": f"/execucoes/{resultado.execucao_id}",
            "job": f"/jobs/{resultado.job_id}",
        },
    )


def _para_resumo(execucao: Execucao) -> ExecucaoResumo:
    return ExecucaoResumo(
        id=execucao.id,
        cenario=execucao.cenario,
        variavel=execucao.variavel,
        arquivo_origem=execucao.arquivo_origem,
        tipo=execucao.tipo,
        status=execucao.status,
        criado_em=execucao.criado_em.isoformat(),
        concluido_em=execucao.concluido_em.isoformat() if execucao.concluido_em else None,
        job_id=execucao.job_id,
    )
