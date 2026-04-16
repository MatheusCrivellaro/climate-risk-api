"""Testes de ``domain/unidades/conversores.py``.

Alguns casos exercitam intencionalmente a heurística ``vmax < 5.0`` do
legado. Esse comportamento é um bug preservado conforme ADR-007 e será
corrigido somente no pós-MVP.
"""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from climate_risk.domain.unidades.conversores import (
    ConversorPrecipitacao,
    ResultadoConversao,
)


def _criar_da(valores: np.ndarray, units: str) -> xr.DataArray:
    return xr.DataArray(valores, dims=("time",), attrs={"units": units})


def test_converte_kg_m2_s1_em_mm_dia() -> None:
    valores = np.array([1e-5, 2e-5, 3e-5], dtype=np.float32)
    da = _criar_da(valores, "kg m-2 s-1")
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)

    assert isinstance(resultado, ResultadoConversao)
    assert resultado.conversao_aplicada is True
    assert resultado.unidade_original == "kg m-2 s-1"
    np.testing.assert_allclose(resultado.dados.values, valores * 86400.0, rtol=1e-6)
    assert resultado.dados.attrs["units"] == "mm/day"


def test_preserva_mm_day_com_vmax_alto() -> None:
    valores = np.array([0.0, 10.0, 30.0, 50.0], dtype=np.float32)
    da = _criar_da(valores, "mm/day")
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)

    assert resultado.conversao_aplicada is False
    np.testing.assert_allclose(resultado.dados.values, valores)
    assert resultado.dados.attrs["units"] == "mm/day"


def test_heuristica_vmax_menor_que_5_converte_mm_day() -> None:
    # Comportamento intencional: uma série em mm/day com vmax < 5.0 é
    # convertida indevidamente pela heurística legada. Bug preservado
    # bit-a-bit conforme ADR-007 — será corrigido apenas pós-MVP.
    valores = np.array([0.5, 1.2, 4.9], dtype=np.float32)
    da = _criar_da(valores, "mm/day")
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)

    assert resultado.conversao_aplicada is True
    np.testing.assert_allclose(resultado.dados.values, valores * 86400.0, rtol=1e-6)


def test_unidade_original_preservada_no_resultado() -> None:
    da = _criar_da(np.array([10.0, 20.0], dtype=np.float32), "mm/day")
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)
    assert resultado.unidade_original == "mm/day"


@pytest.mark.parametrize("units", ["kg m^-2 s^-1", "mm s-1", "mm/s"])
def test_converte_variantes_de_fluxo(units: str) -> None:
    valores = np.array([1e-5, 1e-4], dtype=np.float32)
    da = _criar_da(valores, units)
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)
    assert resultado.conversao_aplicada is True
    np.testing.assert_allclose(resultado.dados.values, valores * 86400.0, rtol=1e-6)


def test_units_ausente_usa_heuristica() -> None:
    da = xr.DataArray(np.array([0.1, 0.2, 0.3], dtype=np.float32), dims=("time",))
    resultado = ConversorPrecipitacao.para_mm_por_dia(da)
    # Sem units e vmax < 5 ⇒ heurística dispara conversão.
    assert resultado.conversao_aplicada is True
    assert resultado.unidade_original == ""
