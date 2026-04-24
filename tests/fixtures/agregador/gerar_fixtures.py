"""Gera fixtures sintéticas para o agregador espacial.

Gera, idempotentemente, sob ``tests/fixtures/agregador/``:

- ``mun_sintetico.shp`` (+ arquivos auxiliares): dois municípios retangulares
  adjacentes, IDs fictícios ``9999999`` (A, oeste) e ``8888888`` (B, leste).
- ``grade_regular_1d.nc``: DataArray 5x5 em grade regular 1D, com valores
  1.0 no município A e 2.0 em B, em 3 timestamps.
- ``grade_2d.nc``: DataArray 3x3 com ``lat``/``lon`` 2D pré-calculados,
  simulando arquivo rotated-pole que já traz as coordenadas geográficas.

Rodar via ``uv run python tests/fixtures/agregador/gerar_fixtures.py``.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

DIRETORIO = Path(__file__).resolve().parent


def gerar_shapefile(destino: Path) -> None:
    """Dois municípios retangulares adjacentes em [0, 10]° lat x [-50, -30]° lon."""
    municipio_a = box(-50.0, 0.0, -40.0, 10.0)
    municipio_b = box(-40.0, 0.0, -30.0, 10.0)
    gdf = gpd.GeoDataFrame(
        {"CD_MUN": ["9999999", "8888888"]},
        geometry=[municipio_a, municipio_b],
        crs="EPSG:4326",
    )
    gdf.to_file(destino, driver="ESRI Shapefile")


def gerar_grade_regular_1d(destino: Path) -> None:
    """Grade 5x5 com lat/lon 1D. Colunas 0-1 em A (1.0), 2-4 em B (2.0).

    lat vai de 1.0 a 9.0 (passo 2°), lon vai de -49.0 a -31.0 (passo 4.5°).
    Com lon = [-49, -44.5, -40, -35.5, -31], duas primeiras colunas caem
    dentro do município A ([-50, -40]) e as três últimas dentro de B ([-40, -30]).
    """
    lat = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
    lon = np.array([-49.0, -44.5, -40.0 + 1e-6, -35.5, -31.0])
    # Pequeno epsilon para evitar o ponto exatamente em -40° (borda entre A e B).
    tempo = pd.date_range("2030-01-01", periods=3, freq="D")

    valores = np.zeros((3, 5, 5), dtype=np.float64)
    # Colunas 0-1 são A, 2-4 são B.
    valores[:, :, 0:2] = 1.0
    valores[:, :, 2:5] = 2.0

    da = xr.DataArray(
        valores,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="pr",
    )
    da.to_netcdf(destino)


def gerar_grade_2d(destino: Path) -> None:
    """Grade 3x3 com coordenadas ``lat`` e ``lon`` 2D pré-calculadas.

    Simula arquivo rotated-pole do SMHI: dims são ``rlat``/``rlon``, mas as
    coordenadas geográficas reais são expostas como ``lat``/``lon`` 2D.
    Todos os 9 pontos ficam dentro do município A (oeste).
    """
    rlat = np.array([0.0, 1.0, 2.0])
    rlon = np.array([0.0, 1.0, 2.0])
    lat2d = np.array(
        [
            [2.0, 2.0, 2.0],
            [5.0, 5.0, 5.0],
            [8.0, 8.0, 8.0],
        ]
    )
    lon2d = np.array(
        [
            [-48.0, -46.0, -44.0],
            [-48.0, -46.0, -44.0],
            [-48.0, -46.0, -44.0],
        ]
    )
    tempo = pd.date_range("2030-01-01", periods=3, freq="D")

    valores = np.full((3, 3, 3), 7.0, dtype=np.float64)
    da = xr.DataArray(
        valores,
        dims=("time", "rlat", "rlon"),
        coords={
            "time": tempo,
            "rlat": rlat,
            "rlon": rlon,
            "lat": (("rlat", "rlon"), lat2d),
            "lon": (("rlat", "rlon"), lon2d),
        },
        name="pr",
    )
    da.to_netcdf(destino)


def main() -> None:
    DIRETORIO.mkdir(parents=True, exist_ok=True)
    gerar_shapefile(DIRETORIO / "mun_sintetico.shp")
    gerar_grade_regular_1d(DIRETORIO / "grade_regular_1d.nc")
    gerar_grade_2d(DIRETORIO / "grade_2d.nc")
    print(f"Fixtures geradas em {DIRETORIO}")


if __name__ == "__main__":
    main()
