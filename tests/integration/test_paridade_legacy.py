"""Teste de regressão de paridade numérica com o código legado.

Objetivo: garantir que o novo domínio (``calcular_indices_anuais``)
reproduz bit-a-bit (dentro das tolerâncias acordadas) o comportamento do
script legado original ``gera_pontos_fornecedores.py`` para os mesmos
inputs.

O teste reimplementa inline as funções relevantes do legado (copiadas
do próprio arquivo legado antes de sua remoção na Slice 12). Funciona
como *golden baseline* permanente, sem depender do diretório ``legacy/``
(removido após a paridade ter sido validada no Marco M4). Qualquer
divergência futura quebra a build.

O fixture usado é ``tests/fixtures/netcdf_mini/cordex_sintetico_basico.nc``,
que utiliza calendário padrão. O arquivo cftime NÃO é usado aqui porque o
legado possui bug conhecido com calendário 360_day (ver baseline
``baseline_grade_cftime.csv`` congelada como header-only no Slice 0).
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from climate_risk.domain.espacial.grade import coords_to_2d
from climate_risk.domain.espacial.longitude import ensure_lon_negpos180
from climate_risk.domain.indices.calculadora import (
    IndicesAnuais,
    ParametrosIndices,
    calcular_indices_anuais,
)
from climate_risk.domain.indices.p95 import PeriodoBaseline, calcular_p95_por_celula
from climate_risk.domain.unidades.conversores import ConversorPrecipitacao

FIXTURE_NC = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "netcdf_mini"
    / "cordex_sintetico_basico.nc"
)

RTOL = 1e-6
ATOL = 1e-9


# ---------------------------------------------------------------------------
# Helpers legados reimplementados inline (cópia de gera_pontos_fornecedores.py).
# ---------------------------------------------------------------------------


def _legacy_convert_pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    units = (da.attrs.get("units", "") or "").lower()
    vmax = float(da.max())
    if (
        ("kg m-2 s-1" in units)
        or ("kg m^-2 s^-1" in units)
        or ("mm s-1" in units)
        or ("mm/s" in units)
        or vmax < 5.0
    ):
        da = da * 86400.0
    da.attrs["units"] = "mm/day"
    return da


def _legacy_annual_indices_for_series(
    series: np.ndarray,
    wet_thr: float,
    p95_thr: float | None,
    r_heavy: tuple[float, float],
) -> tuple[int, float, float, float, int, int, float, float]:
    x = np.asarray(series, dtype=np.float32)
    valid = np.isfinite(x)
    if not np.any(valid):
        return 0, np.nan, np.nan, np.nan, 0, 0, np.nan, np.nan
    x = x.copy()
    x[~valid] = 0.0

    wet_mask = x >= wet_thr
    wet_days = int(wet_mask.sum())
    sdii = float(x[wet_mask].mean()) if wet_days > 0 else np.nan

    rx1day = float(x.max()) if x.size > 0 else np.nan

    if x.size >= 5:
        k = np.ones(5, dtype=np.float32)
        acc5 = np.convolve(x, k, mode="valid")
        rx5day = float(acc5.max())
    else:
        rx5day = np.nan

    r20mm = int((x >= r_heavy[0]).sum())
    r50mm = int((x >= r_heavy[1]).sum())

    if p95_thr is not None:
        heavy = x > p95_thr
        r95ptot_mm = float(x[heavy].sum())
        tot = float(x.sum())
        r95ptot_frac = (r95ptot_mm / tot) if tot > 0 else np.nan
    else:
        r95ptot_mm = np.nan
        r95ptot_frac = np.nan

    return wet_days, sdii, rx1day, rx5day, r20mm, r50mm, r95ptot_mm, r95ptot_frac


def _legacy_compute_p95_grid(
    pr_da: xr.DataArray, baseline: tuple[int, int] | None, p95_wet_thr: float
) -> np.ndarray | None:
    if baseline is None:
        return None
    years = pr_da["time"].dt.year
    mask = (years >= baseline[0]) & (years <= baseline[1])
    da_base = pr_da.sel(time=mask)
    if da_base.sizes.get("time", 0) == 0:
        return None
    da_wet = da_base.where(da_base >= p95_wet_thr)
    thr = da_wet.quantile(0.95, dim="time", skipna=True)
    return thr.values.astype("float32")


# ---------------------------------------------------------------------------
# Teste principal de paridade.
# ---------------------------------------------------------------------------


PARAMETROS = ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0))
BASELINE_TUPLA = (2026, 2030)
BASELINE = PeriodoBaseline(inicio=2026, fim=2030)
P95_WET_THR = 1.0

# Pontos fixos do grid 10x10 para amostragem (iy, ix).
PONTOS_FIXOS: tuple[tuple[int, int], ...] = ((2, 2), (5, 5), (8, 8))


def _comparar_float(valor_novo: float, valor_legacy: float, contexto: str) -> None:
    if math.isnan(valor_legacy):
        assert math.isnan(valor_novo), f"{contexto}: esperado NaN, obtido {valor_novo}"
        return
    assert not math.isnan(valor_novo), f"{contexto}: esperado {valor_legacy}, obtido NaN"
    assert math.isclose(valor_novo, valor_legacy, rel_tol=RTOL, abs_tol=ATOL), (
        f"{contexto}: novo={valor_novo}, legacy={valor_legacy}"
    )


def _comparar_indices(
    novo: IndicesAnuais,
    legacy: tuple[int, float, float, float, int, int, float, float],
    contexto: str,
) -> None:
    (
        wet_days_l,
        sdii_l,
        rx1_l,
        rx5_l,
        r20_l,
        r50_l,
        r95mm_l,
        r95frac_l,
    ) = legacy
    assert novo.wet_days == wet_days_l, f"{contexto} wet_days: {novo.wet_days} vs {wet_days_l}"
    assert novo.r20mm == r20_l, f"{contexto} r20mm: {novo.r20mm} vs {r20_l}"
    assert novo.r50mm == r50_l, f"{contexto} r50mm: {novo.r50mm} vs {r50_l}"
    _comparar_float(novo.sdii, sdii_l, f"{contexto} sdii")
    _comparar_float(novo.rx1day, rx1_l, f"{contexto} rx1day")
    _comparar_float(novo.rx5day, rx5_l, f"{contexto} rx5day")
    _comparar_float(novo.r95ptot_mm, r95mm_l, f"{contexto} r95ptot_mm")
    _comparar_float(novo.r95ptot_frac, r95frac_l, f"{contexto} r95ptot_frac")


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
def test_paridade_novo_dominio_vs_legacy_em_fixture_basica() -> None:
    ds = xr.open_dataset(FIXTURE_NC)
    try:
        pr = ds["pr"]

        # Conversão de unidade — ambos os caminhos.
        pr_novo = ConversorPrecipitacao.para_mm_por_dia(pr.copy(deep=True)).dados
        pr_legacy = _legacy_convert_pr_to_mm_per_day(pr.copy(deep=True))

        # Valores convertidos devem coincidir exatamente.
        np.testing.assert_allclose(pr_novo.values, pr_legacy.values, rtol=RTOL, atol=ATOL)

        # Coordenadas 2D.
        lat_vals = np.asarray(ds["lat"].values)
        lon_vals = np.asarray(ds["lon"].values)
        lat2d, lon2d = coords_to_2d(lat_vals, lon_vals)
        _ = ensure_lon_negpos180(lon2d.reshape(-1))  # usa função do domínio.

        # P95 por célula — comparar arrays.
        p95_novo = calcular_p95_por_celula(pr_novo, BASELINE, P95_WET_THR)
        p95_legacy = _legacy_compute_p95_grid(pr_legacy, BASELINE_TUPLA, P95_WET_THR)
        assert p95_novo is not None and p95_legacy is not None
        np.testing.assert_allclose(p95_novo, p95_legacy, rtol=RTOL, atol=ATOL)

        anos = np.unique(pr_novo["time"].dt.year.values)
        assert anos.size > 0

        p95_flat_novo = p95_novo.reshape(-1)
        p95_flat_legacy = p95_legacy.reshape(-1)
        _, nx = lat2d.shape

        for iy, ix in PONTOS_FIXOS:
            k = iy * nx + ix
            p95_thr_novo = float(p95_flat_novo[k]) if np.isfinite(p95_flat_novo[k]) else None
            p95_thr_legacy = float(p95_flat_legacy[k]) if np.isfinite(p95_flat_legacy[k]) else None

            serie_novo = pr_novo.isel(lat=iy, lon=ix).values
            serie_legacy = pr_legacy.isel(lat=iy, lon=ix).values
            anos_serie = pr_novo["time"].dt.year.values

            for ano in anos:
                mask = anos_serie == ano
                fatia_novo = serie_novo[mask]
                fatia_legacy = serie_legacy[mask]

                indices_novo = calcular_indices_anuais(fatia_novo, PARAMETROS, p95_thr=p95_thr_novo)
                indices_legacy = _legacy_annual_indices_for_series(
                    fatia_legacy,
                    wet_thr=PARAMETROS.freq_thr_mm,
                    p95_thr=p95_thr_legacy,
                    r_heavy=PARAMETROS.heavy_thresholds,
                )
                _comparar_indices(
                    indices_novo,
                    indices_legacy,
                    f"ponto=({iy},{ix}) ano={ano}",
                )
    finally:
        ds.close()
