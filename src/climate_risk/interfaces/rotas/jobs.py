"""Rotas administrativas da fila (``/jobs``)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from climate_risk.application.jobs.consultar import ConsultarJobs
from climate_risk.application.jobs.reprocessar import ReprocessarJob
from climate_risk.domain.entidades.job import Job
from climate_risk.interfaces.dependencias import (
    obter_consultar_jobs,
    obter_reprocessar_job,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.jobs import JobResponse, ListaJobsResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])


ConsultarJobsDep = Annotated[ConsultarJobs, Depends(obter_consultar_jobs)]
ReprocessarJobDep = Annotated[ReprocessarJob, Depends(obter_reprocessar_job)]


@router.get(
    "",
    response_model=ListaJobsResponse,
    status_code=status.HTTP_200_OK,
    summary="Lista jobs com filtros opcionais por status/tipo.",
)
async def listar_jobs(
    caso_uso: ConsultarJobsDep,
    status_filtro: str | None = Query(default=None, alias="status"),
    tipo: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> ListaJobsResponse:
    resultado = await caso_uso.listar(status=status_filtro, tipo=tipo, limit=limit, offset=offset)
    return ListaJobsResponse(
        total=resultado.total,
        limit=resultado.limit,
        offset=resultado.offset,
        items=[_para_response(j) for j in resultado.items],
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    status_code=status.HTTP_200_OK,
    summary="Obtém um job pelo id.",
    responses={
        404: {"model": ProblemDetails, "description": "Job não encontrado."},
    },
)
async def obter_job(job_id: str, caso_uso: ConsultarJobsDep) -> JobResponse:
    job = await caso_uso.buscar_por_id(job_id)
    return _para_response(job)


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    status_code=status.HTTP_200_OK,
    summary="Reprocessa um job que está em estado 'failed'.",
    responses={
        404: {"model": ProblemDetails, "description": "Job não encontrado."},
        409: {"model": ProblemDetails, "description": "Estado atual não permite retry."},
    },
)
async def reprocessar_job(job_id: str, caso_uso: ReprocessarJobDep) -> JobResponse:
    job = await caso_uso.executar(job_id)
    return _para_response(job)


def _para_response(job: Job) -> JobResponse:
    return JobResponse(
        id=job.id,
        tipo=job.tipo,
        payload=job.payload,
        status=job.status,
        tentativas=job.tentativas,
        max_tentativas=job.max_tentativas,
        criado_em=job.criado_em,
        iniciado_em=job.iniciado_em,
        concluido_em=job.concluido_em,
        heartbeat=job.heartbeat,
        erro=job.erro,
        proxima_tentativa_em=job.proxima_tentativa_em,
    )
