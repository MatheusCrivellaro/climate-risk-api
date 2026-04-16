"""Testes de ``domain/indices/calculadora.py``."""

from __future__ import annotations

import math

import numpy as np
import pytest

from climate_risk.domain.indices.calculadora import (
    ParametrosIndices,
    calcular_indices_anuais,
)

PARAMETROS_PADRAO = ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0))


def test_series_toda_zero() -> None:
    series = np.zeros(365, dtype=np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.wet_days == 0
    assert math.isnan(r.sdii)
    assert r.rx1day == 0.0
    assert r.rx5day == 0.0
    assert r.r20mm == 0
    assert r.r50mm == 0


def test_unico_dia_chuvoso_abaixo_do_threshold() -> None:
    series = np.zeros(30, dtype=np.float32)
    series[10] = 10.0  # < 20
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.wet_days == 0
    assert math.isnan(r.sdii)
    assert r.rx1day == pytest.approx(10.0)


def test_unico_dia_chuvoso_acima_do_threshold() -> None:
    series = np.zeros(30, dtype=np.float32)
    series[5] = 25.0
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.wet_days == 1
    assert r.sdii == pytest.approx(25.0)
    assert r.rx1day == pytest.approx(25.0)
    assert r.r20mm == 1
    assert r.r50mm == 0


def test_rx5day_cinco_dias_consecutivos() -> None:
    series = np.zeros(20, dtype=np.float32)
    series[3:8] = 10.0  # 5 dias x 10 mm
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.rx5day == pytest.approx(50.0)


def test_rx1day_igual_ao_maximo() -> None:
    rng = np.random.default_rng(seed=123)
    series = rng.uniform(0.0, 30.0, size=200).astype(np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.rx1day == pytest.approx(float(series.max()))


def test_r95ptot_com_p95_conhecido() -> None:
    series = np.array([1.0, 2.0, 3.0, 10.0, 20.0], dtype=np.float32)
    params = ParametrosIndices(freq_thr_mm=1.0, heavy_thresholds=(20.0, 50.0))
    r = calcular_indices_anuais(series, params, p95_thr=5.0)
    assert r.r95ptot_mm == pytest.approx(30.0)
    assert r.r95ptot_frac == pytest.approx(30.0 / 36.0)


def test_r95ptot_retorna_nan_sem_p95_thr() -> None:
    series = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO, p95_thr=None)
    assert math.isnan(r.r95ptot_mm)
    assert math.isnan(r.r95ptot_frac)


def test_series_vazia() -> None:
    series = np.array([], dtype=np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    # Todos valid mask = False → early return: wet_days=0, restante NaN.
    assert r.wet_days == 0
    assert math.isnan(r.sdii)
    assert math.isnan(r.rx1day)
    assert math.isnan(r.rx5day)
    assert r.r20mm == 0
    assert r.r50mm == 0
    assert math.isnan(r.r95ptot_mm)
    assert math.isnan(r.r95ptot_frac)


def test_nan_no_meio_da_serie_e_tratado_como_zero() -> None:
    # O legado aplica arr[~valid] = 0.0; comportamento preservado.
    series = np.array([10.0, np.nan, 25.0, np.nan, 30.0], dtype=np.float32)
    params = ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0))
    r = calcular_indices_anuais(series, params)
    assert r.wet_days == 2  # 25 e 30
    assert r.sdii == pytest.approx((25.0 + 30.0) / 2)
    assert r.rx1day == pytest.approx(30.0)


def test_propriedade_wet_days_nao_negativo() -> None:
    rng = np.random.default_rng(seed=7)
    series = rng.uniform(-5.0, 40.0, size=365).astype(np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert r.wet_days >= 0


def test_propriedade_r95ptot_frac_entre_0_e_1_ou_nan() -> None:
    rng = np.random.default_rng(seed=42)
    series = rng.uniform(0.0, 50.0, size=365).astype(np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO, p95_thr=10.0)
    assert math.isnan(r.r95ptot_frac) or (0.0 <= r.r95ptot_frac <= 1.0)


def test_serie_menor_que_5_dias_tem_rx5day_nan() -> None:
    series = np.array([10.0, 20.0, 30.0], dtype=np.float32)
    r = calcular_indices_anuais(series, PARAMETROS_PADRAO)
    assert math.isnan(r.rx5day)
    assert r.rx1day == pytest.approx(30.0)
