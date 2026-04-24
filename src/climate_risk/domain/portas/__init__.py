"""Portas (interfaces) do domínio.

As portas são contratos que a camada ``application`` usa e a camada
``infrastructure`` implementa. Definidas como ``typing.Protocol``.
"""

from climate_risk.domain.portas.fila_jobs import FilaJobs
from climate_risk.domain.portas.leitor_netcdf import LeitorNetCDF
from climate_risk.domain.portas.repositorio_resultado_estresse_hidrico import (
    RepositorioResultadoEstresseHidrico,
)
from climate_risk.domain.portas.repositorios import (
    RepositorioExecucoes,
    RepositorioFornecedores,
    RepositorioJobs,
    RepositorioMunicipios,
    RepositorioResultados,
)
from climate_risk.domain.portas.shapefile_municipios import (
    LocalizacaoGeografica,
    ShapefileMunicipios,
)

__all__ = [
    "FilaJobs",
    "LeitorNetCDF",
    "LocalizacaoGeografica",
    "RepositorioExecucoes",
    "RepositorioFornecedores",
    "RepositorioJobs",
    "RepositorioMunicipios",
    "RepositorioResultadoEstresseHidrico",
    "RepositorioResultados",
    "ShapefileMunicipios",
]
