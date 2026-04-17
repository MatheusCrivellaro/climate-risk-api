"""Entidades de domínio (dataclasses puras, sem SQLAlchemy)."""

from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.entidades.resultado import ResultadoIndice

__all__ = [
    "DadosClimaticos",
    "Execucao",
    "Fornecedor",
    "Job",
    "Municipio",
    "ResultadoIndice",
    "StatusExecucao",
    "StatusJob",
]
