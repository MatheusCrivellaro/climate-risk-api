"""Testes de ``domain/indices/p95.py``."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.domain.indices.p95 import (
    PeriodoBaseline,
    calcular_p95_por_celula,
    calcular_p95_por_celula_numpy,
)


def _criar_da_diario(anos: range, ny: int, nx: int, seed: int = 42) -> xr.DataArray:
    rng = np.random.default_rng(seed=seed)
    inicio = f"{anos[0]}-01-01"
    fim = f"{anos[-1]}-12-31"
    tempo = pd.date_range(inicio, fim, freq="D")
    valores = rng.uniform(0.0, 30.0, size=(len(tempo), ny, nx)).astype(np.float32)
    return xr.DataArray(
        valores,
        dims=("time", "y", "x"),
        coords={"time": tempo},
        attrs={"units": "mm/day"},
    )


def test_periodo_baseline_valido() -> None:
    baseline = PeriodoBaseline(inicio=2026, fim=2030)
    assert baseline.inicio == 2026
    assert baseline.fim == 2030


def test_periodo_baseline_invalido_inicio_maior_que_fim() -> None:
    with pytest.raises(ValueError, match="inicio"):
        PeriodoBaseline(inicio=2030, fim=2026)


def test_calcular_p95_retorna_none_com_baseline_none() -> None:
    da = _criar_da_diario(range(2026, 2029), ny=3, nx=3)
    assert calcular_p95_por_celula(da, baseline=None, p95_wet_thr=1.0) is None


def test_calcular_p95_retorna_array_2d_com_valores_finitos() -> None:
    ny, nx = 5, 5
    da = _criar_da_diario(range(2026, 2029), ny=ny, nx=nx)
    baseline = PeriodoBaseline(inicio=2026, fim=2028)

    p95 = calcular_p95_por_celula(da, baseline=baseline, p95_wet_thr=1.0)
    assert p95 is not None
    assert p95.shape == (ny, nx)
    assert p95.dtype == np.float32
    assert np.any(np.isfinite(p95))


def test_calcular_p95_baseline_fora_do_range_retorna_none() -> None:
    da = _criar_da_diario(range(2026, 2029), ny=3, nx=3)
    baseline = PeriodoBaseline(inicio=2050, fim=2052)
    assert calcular_p95_por_celula(da, baseline=baseline, p95_wet_thr=1.0) is None


def test_calcular_p95_threshold_alto_filtra_todos_os_valores() -> None:
    # Dados limitados a 0..30 mm/day: threshold de 100 filtra tudo e o
    # quantil fica NaN em todas as células.
    ny, nx = 3, 3
    da = _criar_da_diario(range(2026, 2027), ny=ny, nx=nx)
    baseline = PeriodoBaseline(inicio=2026, fim=2026)
    p95 = calcular_p95_por_celula(da, baseline=baseline, p95_wet_thr=100.0)
    assert p95 is not None
    assert p95.shape == (ny, nx)
    assert np.all(np.isnan(p95))


# -------------------------------------------------------------------
# Equivalência xarray ↔ numpy (base do Slice 4).
# -------------------------------------------------------------------


def test_variante_numpy_retorna_none_quando_baseline_none() -> None:
    dados = np.zeros((3, 2, 2), dtype=np.float32)
    anos = np.array([2026, 2027, 2028], dtype=np.int64)
    assert calcular_p95_por_celula_numpy(dados, anos, baseline=None, p95_wet_thr=1.0) is None


def test_variante_numpy_retorna_none_quando_baseline_fora_do_range() -> None:
    dados = np.zeros((3, 2, 2), dtype=np.float32)
    anos = np.array([2026, 2027, 2028], dtype=np.int64)
    baseline = PeriodoBaseline(inicio=2050, fim=2052)
    assert calcular_p95_por_celula_numpy(dados, anos, baseline=baseline, p95_wet_thr=1.0) is None


def test_variante_numpy_equivale_a_versao_xarray() -> None:
    ny, nx = 5, 5
    da = _criar_da_diario(range(2026, 2029), ny=ny, nx=nx)
    baseline = PeriodoBaseline(inicio=2026, fim=2028)

    p95_xr = calcular_p95_por_celula(da, baseline=baseline, p95_wet_thr=1.0)

    dados_np = np.asarray(da.values)
    anos_np = np.asarray(da["time"].dt.year.values, dtype=np.int64)
    p95_np = calcular_p95_por_celula_numpy(dados_np, anos_np, baseline=baseline, p95_wet_thr=1.0)

    assert p95_xr is not None and p95_np is not None
    np.testing.assert_allclose(p95_np, p95_xr, rtol=1e-6, atol=1e-9)


def test_variante_numpy_threshold_alto_gera_nan_por_celula() -> None:
    ny, nx = 3, 3
    rng = np.random.default_rng(seed=1)
    dados = rng.uniform(0.0, 30.0, size=(365, ny, nx)).astype(np.float32)
    anos = np.full(365, 2026, dtype=np.int64)
    baseline = PeriodoBaseline(inicio=2026, fim=2026)
    p95 = calcular_p95_por_celula_numpy(dados, anos, baseline=baseline, p95_wet_thr=100.0)
    assert p95 is not None
    assert p95.shape == (ny, nx)
    assert np.all(np.isnan(p95))
