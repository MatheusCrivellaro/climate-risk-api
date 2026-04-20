"""Rotas administrativas.

- ``POST /admin/ibge/refresh`` (Slice 8): repovoa o cache de municípios a
  partir da API do IBGE.
- ``GET /admin/stats`` (Slice 12): overview operacional — counters de
  entidades persistidas + estatísticas de :class:`ConsultarStats`.

Tudo fica sob ``/admin`` para permitir proteção futura por autenticação
sem afetar o domínio.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from climate_risk.application.geocodificacao import RefreshCatalogoIBGE
from climate_risk.application.resultados import ConsultarStats
from climate_risk.interfaces.dependencias import (
    RepoExecucoesDep,
    RepoFornecedoresDep,
    RepoJobsDep,
    RepoMunicipiosDep,
    obter_caso_uso_consultar_stats,
    obter_caso_uso_refresh_ibge,
)
from climate_risk.interfaces.schemas.admin import AdminStatsResponse, ContadoresAdmin
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.geocoding import RefreshIBGEResponse

router = APIRouter(prefix="/admin", tags=["admin"])

RefreshDep = Annotated[RefreshCatalogoIBGE, Depends(obter_caso_uso_refresh_ibge)]
StatsDep = Annotated[ConsultarStats, Depends(obter_caso_uso_consultar_stats)]


@router.post(
    "/ibge/refresh",
    response_model=RefreshIBGEResponse,
    summary="Recarrega o cache de municípios a partir da API do IBGE.",
    responses={
        503: {"model": ProblemDetails, "description": "API do IBGE indisponível."},
    },
)
async def refresh_ibge(caso: RefreshDep) -> RefreshIBGEResponse:
    """Baixa o catálogo completo (~5570 municípios) e atualiza o cache.

    Operação cara — uso típico é esporádico (provisionamento, upgrade
    anual do IBGE). Nenhuma autenticação por enquanto: o endpoint ``/admin``
    serve de placeholder para uma camada futura.
    """
    resultado = await caso.executar()
    return RefreshIBGEResponse(
        total_municipios=resultado.total_municipios,
        com_centroide=resultado.com_centroide,
        sem_centroide=resultado.sem_centroide,
    )


@router.get(
    "/stats",
    response_model=AdminStatsResponse,
    summary="Sumário operacional: counters + estatísticas dos resultados.",
)
async def admin_stats(
    caso_stats: StatsDep,
    repo_fornecedores: RepoFornecedoresDep,
    repo_municipios: RepoMunicipiosDep,
    repo_jobs: RepoJobsDep,
    repo_execucoes: RepoExecucoesDep,
) -> AdminStatsResponse:
    """Overview para dashboards — counters básicos + distinct values dos índices.

    Reúsa :class:`ConsultarStats` (Slice 11) e adiciona contadores simples
    sobre as quatro entidades principais. Nenhuma consulta é cara: todos
    os ``SELECT COUNT(*)`` rodam direto sobre índices primários.
    """
    estatisticas = await caso_stats.executar()
    contadores = ContadoresAdmin(
        fornecedores=await repo_fornecedores.contar(),
        municipios=await repo_municipios.contar(),
        jobs=await repo_jobs.contar(),
        execucoes=await repo_execucoes.contar(),
    )
    return AdminStatsResponse(
        contadores=contadores,
        cenarios=estatisticas.cenarios,
        anos=estatisticas.anos,
        variaveis=estatisticas.variaveis,
        nomes_indices=estatisticas.nomes_indices,
        total_execucoes_com_resultados=estatisticas.total_execucoes_com_resultados,
        total_resultados=estatisticas.total_resultados,
    )
