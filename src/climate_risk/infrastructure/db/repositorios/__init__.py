"""Implementações concretas dos repositórios (SQLAlchemy async)."""

from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.fornecedores import (
    SQLAlchemyRepositorioFornecedores,
)
from climate_risk.infrastructure.db.repositorios.jobs import (
    SQLAlchemyRepositorioJobs,
)
from climate_risk.infrastructure.db.repositorios.municipios import (
    SQLAlchemyRepositorioMunicipios,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)

__all__ = [
    "SQLAlchemyRepositorioExecucoes",
    "SQLAlchemyRepositorioFornecedores",
    "SQLAlchemyRepositorioJobs",
    "SQLAlchemyRepositorioMunicipios",
    "SQLAlchemyRepositorioResultados",
]
