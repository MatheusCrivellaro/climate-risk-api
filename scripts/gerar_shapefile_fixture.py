"""Gera o shapefile sintético ``tests/fixtures/shapefile/municipios_minimo.shp``.

O arquivo produzido contém 5 polígonos triviais (quadrados lat/lon) com as
mesmas colunas usadas pelo IBGE — ``CD_MUN``/``SIGLA_UF``/``NM_MUN`` — para
exercitar :class:`ShapefileGeopandas` sem depender do shapefile real do
IBGE (que pesa >100 MB).

Rode manualmente quando precisar recriar os artefatos::

    uv run python scripts/gerar_shapefile_fixture.py

Os arquivos gerados (``.shp``, ``.shx``, ``.dbf``, ``.prj``, ``.cpg``) são
pequenos (~5 KB total) e ficam versionados em ``tests/fixtures/shapefile/``.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "shapefile"
FIXTURE_PATH = FIXTURE_DIR / "municipios_minimo.shp"


def _quadrado(lat_centro: float, lon_centro: float, lado: float = 0.4) -> Polygon:
    """Quadrado com ``lado`` graus centrado em ``(lat_centro, lon_centro)``."""
    meio = lado / 2
    return Polygon(
        [
            (lon_centro - meio, lat_centro - meio),
            (lon_centro + meio, lat_centro - meio),
            (lon_centro + meio, lat_centro + meio),
            (lon_centro - meio, lat_centro + meio),
        ]
    )


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    registros = [
        {
            "CD_MUN": 3550308,
            "SIGLA_UF": "SP",
            "NM_MUN": "São Paulo",
            "geometry": _quadrado(-23.55, -46.63),
        },
        {
            "CD_MUN": 3304557,
            "SIGLA_UF": "RJ",
            "NM_MUN": "Rio de Janeiro",
            "geometry": _quadrado(-22.90, -43.20),
        },
        {
            "CD_MUN": 3106200,
            "SIGLA_UF": "MG",
            "NM_MUN": "Belo Horizonte",
            "geometry": _quadrado(-19.92, -43.94),
        },
        {
            "CD_MUN": 4106902,
            "SIGLA_UF": "PR",
            "NM_MUN": "Curitiba",
            "geometry": _quadrado(-25.42, -49.27),
        },
        {
            "CD_MUN": 4314902,
            "SIGLA_UF": "RS",
            "NM_MUN": "Porto Alegre",
            "geometry": _quadrado(-30.03, -51.23),
        },
    ]
    gdf = gpd.GeoDataFrame(registros, crs="EPSG:4326")
    gdf.to_file(FIXTURE_PATH, driver="ESRI Shapefile")
    print(f"Shapefile fixture gravado em {FIXTURE_PATH}")


if __name__ == "__main__":
    main()
