"""Portas (interfaces) do domínio.

As portas são contratos que a camada ``application`` usa e a camada
``infrastructure`` implementa. Definidas como ``typing.Protocol``.
"""

from climate_risk.domain.portas.repositorios import (
    RepositorioExecucoes,
    RepositorioFornecedores,
    RepositorioJobs,
    RepositorioMunicipios,
    RepositorioResultados,
)

__all__ = [
    "RepositorioExecucoes",
    "RepositorioFornecedores",
    "RepositorioJobs",
    "RepositorioMunicipios",
    "RepositorioResultados",
]
