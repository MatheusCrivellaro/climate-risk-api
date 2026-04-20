"""Cálculo do limiar P95 por célula sobre o período de baseline.

Portado do script legado ``gera_pontos_fornecedores.py`` (função
``compute_p95_grid``) conforme ADR-001. A versão de
``cordex_pr_freq_intensity.py`` NÃO é usada porque contém o bug
conhecido de incompatibilidade com calendários ``cftime`` (360_day) —
ver histórico do Slice 0 e a baseline header-only em
``tests/fixtures/baselines/sintetica/baseline_grade_cftime.csv``. Código
legado removido na Slice 12.
"""

from __future__ import annotations

import warnings
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


def calcular_p95_por_celula_numpy(
    dados_diarios: np.ndarray,
    anos_por_dia: np.ndarray,
    baseline: PeriodoBaseline | None,
    p95_wet_thr: float,
) -> np.ndarray | None:
    """Variante ``numpy`` de :func:`calcular_p95_por_celula`.

    Permite que a camada ``application`` chame o cálculo do P95 sem ter de
    reconstruir um :class:`xarray.DataArray` a partir dos arrays já presentes
    em :class:`~climate_risk.domain.entidades.dados_climaticos.DadosClimaticos`
    — o que violaria ADR-005 (``application`` não importa ``xarray``).

    Equivalência com a versão ``xarray``:

    - Ambas filtram dias dentro de ``[baseline.inicio, baseline.fim]``.
    - Ambas mascaram dias com ``pr < p95_wet_thr`` antes do quantil.
    - Ambas computam o quantil 0.95 por célula ignorando NaNs.

    Args:
        dados_diarios: Array 3D ``(tempo, y, x)`` em ``mm/dia``.
        anos_por_dia: Array 1D com o ano de cada timestamp em ``dados_diarios``.
            Deve ter o mesmo comprimento que ``dados_diarios.shape[0]``.
        baseline: Intervalo fechado de anos; ``None`` desativa o P95.
        p95_wet_thr: Limiar de "dia chuvoso" em mm/dia (valores menores viram
            NaN antes do quantil).

    Returns:
        Array 2D ``float32`` ``(y, x)`` com o P95 por célula, ou ``None`` quando
        ``baseline is None`` / sem dias no período.
    """
    if baseline is None:
        return None

    mascara_tempo = (anos_por_dia >= baseline.inicio) & (anos_por_dia <= baseline.fim)
    if not np.any(mascara_tempo):
        return None

    base = np.asarray(dados_diarios[mascara_tempo], dtype=np.float32)
    base_wet = np.where(base >= p95_wet_thr, base, np.nan)
    with warnings.catch_warnings():
        # All-NaN slice emite RuntimeWarning; comportamento equivalente ao
        # ``skipna=True`` do xarray (retorna NaN por célula).
        warnings.simplefilter("ignore", RuntimeWarning)
        quantil = np.nanquantile(base_wet, 0.95, axis=0)
    return np.asarray(quantil, dtype=np.float32)
