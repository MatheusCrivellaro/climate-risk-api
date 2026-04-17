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
from climate_risk.core.config import Settings, get_settings
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.db.sessao import get_sessao
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


LeitorNetCDFDep = Annotated[LeitorXarray, Depends(obter_leitor_netcdf)]
RepoExecucoesDep = Annotated[SQLAlchemyRepositorioExecucoes, Depends(obter_repositorio_execucoes)]
RepoResultadosDep = Annotated[
    SQLAlchemyRepositorioResultados, Depends(obter_repositorio_resultados)
]


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
