"""Testes do streaming :meth:`AgregadorMunicipiosGeopandas.iterar_por_municipio`.

Slice 21 / ADR-013. Reusa as fixtures sintéticas de ``test_agregador_municipios``
(``mun_sintetico.shp`` + grades pré-preenchidas).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "agregador"
SHAPEFILE = FIXTURES / "mun_sintetico.shp"
GRADE_1D = FIXTURES / "grade_regular_1d.nc"

pytestmark = pytest.mark.skipif(
    not SHAPEFILE.exists(),
    reason="Fixtures sintéticas ausentes — rode tests/fixtures/agregador/gerar_fixtures.py.",
)


def test_iterar_retorna_iterator_e_nao_dataframe(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        resultado = agreg.iterar_por_municipio(da)
        assert isinstance(resultado, Iterator)
    finally:
        da.close()


def test_iterar_yield_uma_tupla_por_municipio(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = list(agreg.iterar_por_municipio(da))
    finally:
        da.close()

    assert len(tuplas) == 2
    municipios = {mun for mun, _, _ in tuplas}
    assert municipios == {9999999, 8888888}


def test_iterar_tipos_e_dimensoes_corretos(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = list(agreg.iterar_por_municipio(da))
    finally:
        da.close()

    for municipio_id, datas, serie in tuplas:
        assert isinstance(municipio_id, int)
        assert isinstance(datas, np.ndarray)
        assert isinstance(serie, np.ndarray)
        assert datas.ndim == 1
        assert serie.ndim == 1
        assert len(datas) == len(serie)
        # Grade 1D tem 3 timestamps.
        assert len(datas) == 3


def test_iterar_valores_corretos_media_espacial(tmp_path: Path) -> None:
    """Município A (oeste) tem células com valor 1.0; B (leste) tem 2.0."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = dict((mun, serie) for mun, _, serie in agreg.iterar_por_municipio(da))
    finally:
        da.close()

    np.testing.assert_array_equal(tuplas[9999999], np.array([1.0, 1.0, 1.0]))
    np.testing.assert_array_equal(tuplas[8888888], np.array([2.0, 2.0, 2.0]))


def test_iterar_ordem_deterministica(tmp_path: Path) -> None:
    """Duas chamadas → mesma ordem de municípios. Crítico para sync no handler."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        ordem_1 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
        ordem_2 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
        ordem_3 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
    finally:
        da.close()

    assert ordem_1 == ordem_2 == ordem_3
    # Ordem específica: ASC numérica (8888888 < 9999999).
    assert ordem_1 == [8888888, 9999999]


def test_iterar_grade_sem_municipios_nao_yield(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    # Cobre [20,22]° lat x [-60,-58]° lon — fora do shapefile sintético.
    lat = np.array([20.0, 22.0])
    lon = np.array([-60.0, -58.0])
    da = xr.DataArray(
        np.ones((2, 2, 2), dtype=np.float64),
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    tuplas = list(agreg.iterar_por_municipio(da))
    assert tuplas == []


def test_iterar_serie_nan_preservada(tmp_path: Path) -> None:
    """Quando todas as células de um município estão NaN num dia, ``serie[t]``
    é NaN (a média 'skipna' de zero valores válidos é NaN)."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
    lon = np.array([-49.0, -44.5, -40.0 + 1e-6, -35.5, -31.0])
    valores = np.full((2, 5, 5), 2.0, dtype=np.float64)
    valores[:, :, 0:2] = np.nan  # Município A inteiro NaN.
    da = xr.DataArray(
        valores, dims=("time", "lat", "lon"), coords={"time": tempo, "lat": lat, "lon": lon}
    )

    series = dict((mun, serie) for mun, _, serie in agreg.iterar_por_municipio(da))
    assert np.isnan(series[9999999]).all()  # A: NaN puro.
    np.testing.assert_array_equal(series[8888888], np.array([2.0, 2.0]))  # B: ok.


def test_legado_agregar_continua_devolvendo_dataframe(tmp_path: Path) -> None:
    """Regressão: o método legacy permanece funcionando para os callers antigos."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    finally:
        da.close()

    assert list(df.columns) == ["municipio_id", "data", "valor", "nome_variavel"]
    # IDs continuam strings (vêm do shapefile como string).
    assert set(df["municipio_id"].unique()) == {"9999999", "8888888"}
    assert len(df) == 6
