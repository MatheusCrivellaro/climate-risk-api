"""Dependências compartilhadas do FastAPI.

Expõe providers prontos para ``Depends(...)`` que injetam adaptadores de
``infrastructure`` nos casos de uso de ``application``. Todas as portas
ficam do lado de ``domain``; aqui apenas compomos.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.application.calculos.calcular_por_pontos import CalcularIndicesPorPontos
from climate_risk.application.calculos.criar_execucao_por_pontos import CriarExecucaoPorPontos
from climate_risk.application.execucoes.cancelar import CancelarExecucao
from climate_risk.application.execucoes.consultar import ConsultarExecucoes
from climate_risk.application.execucoes.criar import CriarExecucaoCordex
from climate_risk.application.jobs.consultar import ConsultarJobs
from climate_risk.application.jobs.reprocessar import ReprocessarJob
from climate_risk.core.config import Settings, get_settings
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.jobs import SQLAlchemyRepositorioJobs
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray


def obter_settings() -> Settings:
    """Adapter para ``FastAPI.Depends`` que devolve a configuração global."""
    return get_settings()


def obter_leitor_netcdf() -> LeitorXarray:
    """Instancia o adaptador :class:`LeitorXarray` por request.

    A classe é leve (sem estado mutável relevante) — criar um por request
    evita compartilhar ``FileHandle`` abertos entre corrotinas.
    """
    return LeitorXarray()


SessaoDep = Annotated[AsyncSession, Depends(get_sessao)]


def obter_repositorio_execucoes(sessao: SessaoDep) -> SQLAlchemyRepositorioExecucoes:
    """Repositório de execuções ligado à sessão da request."""
    return SQLAlchemyRepositorioExecucoes(sessao)


def obter_repositorio_resultados(sessao: SessaoDep) -> SQLAlchemyRepositorioResultados:
    """Repositório de resultados ligado à sessão da request."""
    return SQLAlchemyRepositorioResultados(sessao)


def obter_repositorio_jobs(sessao: SessaoDep) -> SQLAlchemyRepositorioJobs:
    """Repositório CRUD de jobs ligado à sessão da request."""
    return SQLAlchemyRepositorioJobs(sessao)


def obter_fila_jobs(sessao: SessaoDep) -> FilaSQLite:
    """Fila de jobs ligada à sessão da request."""
    return FilaSQLite(sessao)


LeitorNetCDFDep = Annotated[LeitorXarray, Depends(obter_leitor_netcdf)]
RepoExecucoesDep = Annotated[SQLAlchemyRepositorioExecucoes, Depends(obter_repositorio_execucoes)]
RepoResultadosDep = Annotated[
    SQLAlchemyRepositorioResultados, Depends(obter_repositorio_resultados)
]
RepoJobsDep = Annotated[SQLAlchemyRepositorioJobs, Depends(obter_repositorio_jobs)]
FilaJobsDep = Annotated[FilaSQLite, Depends(obter_fila_jobs)]


def obter_caso_uso_calcular_por_pontos(
    leitor: LeitorNetCDFDep,
    repo_execucoes: RepoExecucoesDep,
    repo_resultados: RepoResultadosDep,
) -> CalcularIndicesPorPontos:
    """Compõe :class:`CalcularIndicesPorPontos` com dependências concretas."""
    return CalcularIndicesPorPontos(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_execucoes,
        repositorio_resultados=repo_resultados,
    )


def obter_caso_uso_criar_execucao(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CriarExecucaoCordex:
    """Compõe :class:`CriarExecucaoCordex` com repositório + fila."""
    return CriarExecucaoCordex(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_caso_uso_criar_execucao_por_pontos(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CriarExecucaoPorPontos:
    """Compõe :class:`CriarExecucaoPorPontos` (UC-03 assíncrono — Slice 7)."""
    return CriarExecucaoPorPontos(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_consultar_execucoes(repo_execucoes: RepoExecucoesDep) -> ConsultarExecucoes:
    """Compõe :class:`ConsultarExecucoes` sobre o repositório."""
    return ConsultarExecucoes(repositorio=repo_execucoes)


def obter_caso_uso_cancelar_execucao(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CancelarExecucao:
    """Compõe :class:`CancelarExecucao` — cancela execução + job vinculado."""
    return CancelarExecucao(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_consultar_jobs(repo_jobs: RepoJobsDep) -> ConsultarJobs:
    """Compõe :class:`ConsultarJobs` sobre o repositório CRUD."""
    return ConsultarJobs(repositorio=repo_jobs)


def obter_reprocessar_job(repo_jobs: RepoJobsDep) -> ReprocessarJob:
    """Compõe :class:`ReprocessarJob` sobre o repositório CRUD."""
    return ReprocessarJob(repositorio=repo_jobs)
