"""Cálculo do limiar P95 por célula sobre o período de baseline.

Portado de ``legacy/gera_pontos_fornecedores.py`` (função
``compute_p95_grid``) conforme ADR-001. A versão de
``legacy/cordex_pr_freq_intensity.py`` NÃO é usada porque contém o bug
conhecido de incompatibilidade com calendários ``cftime`` (360_day) —
ver histórico do Slice 0 e a baseline header-only em
``tests/fixtures/baselines/sintetica/baseline_grade_cftime.csv``.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import xarray as xr


@dataclass(frozen=True)
class PeriodoBaseline:
    """Intervalo fechado de anos usado como baseline para o P95."""

    inicio: int
    fim: int

    def __post_init__(self) -> None:
        if self.inicio > self.fim:
            raise ValueError(
                "PeriodoBaseline inválido: inicio deve ser <= fim "
                f"(recebido inicio={self.inicio}, fim={self.fim})."
            )


def calcular_p95_por_celula(
    pr_da: xr.DataArray,
    baseline: PeriodoBaseline | None,
    p95_wet_thr: float,
) -> np.ndarray | None:
    """Retorna o P95 por pixel (array 2D ``float32``) ou ``None``.

    - ``baseline is None`` → retorna ``None``.
    - ``baseline`` sem dados no DataArray → retorna ``None``.
    - Caso contrário, filtra os dias com ``pr >= p95_wet_thr`` no período
      do baseline e calcula o quantil 0.95 por célula em ``time``.
    """
    if baseline is None:
        return None

    years = pr_da["time"].dt.year
    mask = (years >= baseline.inicio) & (years <= baseline.fim)
    da_base = pr_da.sel(time=mask)

    if da_base.sizes.get("time", 0) == 0:
        return None

    da_wet = da_base.where(da_base >= p95_wet_thr)
    thr = da_wet.quantile(0.95, dim="time", skipna=True)
    return np.asarray(thr.values, dtype=np.float32)
