"""Testes de :class:`AgregadorMunicipiosGeopandas`.

Usa fixtures sintéticas (2 municípios retangulares conhecidos + grades
pré-preenchidas) geradas por
``tests/fixtures/agregador/gerar_fixtures.py``. Valores de entrada são
escolhidos para que as médias esperadas fiquem em ``{1.0, 2.0, 7.0}``
sem ambiguidade.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from climate_risk.domain.excecoes import (
    ErroGradeDesconhecida,
    ErroShapefileMunicipiosIndisponivel,
)
from climate_risk.infrastructure import agregador_municipios_geopandas as agregador_module
from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "agregador"
SHAPEFILE = FIXTURES / "mun_sintetico.shp"
GRADE_1D = FIXTURES / "grade_regular_1d.nc"
GRADE_2D = FIXTURES / "grade_2d.nc"

pytestmark = pytest.mark.skipif(
    not SHAPEFILE.exists(),
    reason="Fixtures sintéticas ausentes — rode tests/fixtures/agregador/gerar_fixtures.py.",
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _construir_shapefile_em(destino: Path, polígonos: dict[str, object]) -> Path:
    """Gera um shapefile ad-hoc e devolve o path final ``.shp``."""
    gdf = gpd.GeoDataFrame(
        {"CD_MUN": list(polígonos.keys())},
        geometry=list(polígonos.values()),
        crs="EPSG:4326",
    )
    shp = destino / "mun.shp"
    gdf.to_file(shp, driver="ESRI Shapefile")
    return shp


# ---------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------
def test_agregar_grade_regular_1d_com_duas_regioes(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    finally:
        da.close()

    assert set(df["municipio_id"].unique()) == {"9999999", "8888888"}
    # 3 timestamps x 2 municípios = 6 linhas.
    assert len(df) == 6
    assert (df["nome_variavel"] == "pr").all()

    media_a = df[df["municipio_id"] == "9999999"]["valor"]
    media_b = df[df["municipio_id"] == "8888888"]["valor"]
    # Colunas 0-1 do grid (valor 1.0) caem em A; colunas 2-4 (valor 2.0) em B.
    assert list(media_a) == [1.0, 1.0, 1.0]
    assert list(media_b) == [2.0, 2.0, 2.0]


def test_agregar_coordenadas_2d_pre_calculadas(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_2D)
    try:
        df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    finally:
        da.close()

    # Todos os 9 pontos caem no município A; valores constantes 7.0.
    assert set(df["municipio_id"].unique()) == {"9999999"}
    assert len(df) == 3
    assert list(df["valor"]) == [7.0, 7.0, 7.0]


def test_cache_funciona(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    da = xr.open_dataarray(GRADE_1D)
    try:
        df1 = agreg.agregar_por_municipio(da, nome_variavel="pr")
    finally:
        da.close()

    caches = list(tmp_path.glob("mapa_celulas_*.parquet"))
    assert len(caches) == 1

    # Segunda chamada: se sjoin for invocado, explodimos.
    def _sjoin_proibido(*args: object, **kwargs: object) -> object:
        raise AssertionError("sjoin foi chamado — cache não funcionou.")

    monkeypatch.setattr(agregador_module.gpd, "sjoin", _sjoin_proibido)

    da2 = xr.open_dataarray(GRADE_1D)
    try:
        df2 = agreg.agregar_por_municipio(da2, nome_variavel="pr")
    finally:
        da2.close()

    pd.testing.assert_frame_equal(df1.reset_index(drop=True), df2.reset_index(drop=True))


def test_celulas_fora_dos_municipios_sao_descartadas(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    # Grade cobre [20, 22]° lat x [-60, -58]° lon — totalmente fora do shapefile
    # (municípios ficam em [0,10] x [-50,-30]). Esperamos DataFrame vazio, sem erro.
    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([20.0, 22.0])
    lon = np.array([-60.0, -58.0])
    da = xr.DataArray(
        np.ones((2, 2, 2), dtype=np.float64),
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    assert df.empty
    assert list(df.columns) == ["municipio_id", "data", "valor", "nome_variavel"]


def test_municipio_sem_celulas_nao_aparece(tmp_path: Path) -> None:
    # Shapefile com 3 municípios; DataArray só cobre 2 deles.
    shp = _construir_shapefile_em(
        tmp_path,
        {
            "1111111": box(-50.0, 0.0, -45.0, 10.0),
            "2222222": box(-45.0, 0.0, -40.0, 10.0),
            "3333333": box(100.0, 0.0, 110.0, 10.0),  # Pacífico — fora do DataArray.
        },
    )
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    agreg = AgregadorMunicipiosGeopandas(shp, cache_dir)

    tempo = pd.date_range("2030-01-01", periods=1, freq="D")
    lat = np.array([5.0])
    lon = np.array([-47.5, -42.5])
    da = xr.DataArray(
        np.array([[[10.0, 20.0]]]),
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    assert set(df["municipio_id"].unique()) == {"1111111", "2222222"}
    # "3333333" simplesmente não aparece — não é erro.


def test_valor_nan_nas_celulas_propaga_via_nanmean(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    # Shapefile A tem colunas 0-1 do grid 5x5.
    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
    lon = np.array([-49.0, -44.5, -40.0 + 1e-6, -35.5, -31.0])
    valores = np.full((2, 5, 5), 2.0, dtype=np.float64)
    valores[:, :, 0:2] = np.nan  # Todas as células de A viram NaN.
    # Timestamp 0 do município A: todas-NaN → None no output.
    # Timestamp 1 do município A: todas-NaN → None (testa caso all-NaN).
    # Município B: média = 2.0 sempre.
    # Mistura: também injetar alguns NaN parciais em B e checar média resultante.
    valores[0, 0, 2] = np.nan  # Uma célula de B na primeira coluna NaN no t=0.
    da = xr.DataArray(
        valores, dims=("time", "lat", "lon"), coords={"time": tempo, "lat": lat, "lon": lon}
    )

    df = agreg.agregar_por_municipio(da, nome_variavel="pr")

    a_valores = df[df["municipio_id"] == "9999999"]["valor"]
    # Coluna inteira NaN → valores viram NaN (pandas coerce `None` numérico para NaN).
    assert a_valores.isna().all()

    b_valores = df[df["municipio_id"] == "8888888"]["valor"].tolist()
    # Município B continua com média 2.0 (nanmean ignora a célula NaN).
    assert b_valores == [2.0, 2.0]


def test_serie_temporal_preservada(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=10, freq="D")
    lat = np.array([5.0])
    lon = np.array([-45.0])
    # Valor varia no tempo para garantir que não estamos colapsando a dimensão temporal.
    valores = np.arange(10, dtype=np.float64).reshape(10, 1, 1)
    da = xr.DataArray(
        valores, dims=("time", "lat", "lon"), coords={"time": tempo, "lat": lat, "lon": lon}
    )

    df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    # Único município atingido (A).
    assert df["municipio_id"].unique().tolist() == ["9999999"]
    assert len(df) == 10
    assert df["valor"].tolist() == list(range(10))
    assert df["data"].tolist() == list(tempo)


def test_shapefile_inexistente_levanta_erro(tmp_path: Path) -> None:
    with pytest.raises(ErroShapefileMunicipiosIndisponivel):
        AgregadorMunicipiosGeopandas(tmp_path / "nao-existe.shp", tmp_path)


def test_datararray_sem_coords_latlon_levanta_erro(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=1, freq="D")
    # DataArray sem lat/lon, só dims anônimas y/x.
    da = xr.DataArray(
        np.zeros((1, 2, 2)),
        dims=("time", "y", "x"),
        coords={"time": tempo},
    )
    with pytest.raises(ErroGradeDesconhecida):
        agreg.agregar_por_municipio(da, nome_variavel="pr")


def test_hash_consistente_entre_chamadas(tmp_path: Path) -> None:
    lat2d = np.array([[1.0, 2.0], [3.0, 4.0]])
    lon2d = np.array([[-40.0, -39.0], [-40.0, -39.0]])

    h1 = AgregadorMunicipiosGeopandas._hash_grade(lat2d, lon2d)
    h2 = AgregadorMunicipiosGeopandas._hash_grade(lat2d.copy(), lon2d.copy())
    assert h1 == h2
    assert len(h1) == 16


def test_grades_diferentes_geram_caches_separados(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    da_1d = xr.open_dataarray(GRADE_1D)
    da_2d = xr.open_dataarray(GRADE_2D)
    try:
        agreg.agregar_por_municipio(da_1d, nome_variavel="pr")
        agreg.agregar_por_municipio(da_2d, nome_variavel="pr")
    finally:
        da_1d.close()
        da_2d.close()

    caches = sorted(tmp_path.glob("mapa_celulas_*.parquet"))
    assert len(caches) == 2, caches
