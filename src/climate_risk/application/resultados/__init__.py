"""Casos de uso sobre :class:`ResultadoIndice` (Slice 11)."""

from climate_risk.application.resultados.agregar import (
    AgregarResultados,
    FiltrosAgregacao,
    GrupoAgregado,
    ResultadoAgregacao,
)
from climate_risk.application.resultados.consultar import (
    ConsultarResultados,
    FiltrosResultados,
    PaginaResultados,
)
from climate_risk.application.resultados.stats import (
    ConsultarStats,
    EstatisticasResultados,
)

__all__ = [
    "AgregarResultados",
    "ConsultarResultados",
    "ConsultarStats",
    "EstatisticasResultados",
    "FiltrosAgregacao",
    "FiltrosResultados",
    "GrupoAgregado",
    "PaginaResultados",
    "ResultadoAgregacao",
]
