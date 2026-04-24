"""Testes de integração de :class:`AgregadorMunicipiosGeopandas` com shapefile real.

Só rodam quando ``CLIMATE_RISK_SHAPEFILE_MUN_PATH`` aponta para um
shapefile IBGE completo (ex.: ``BR_Municipios_2024.shp``). Caso contrário
são pulados via ``pytest.skip``. Marcados como ``slow`` porque carregar
o shapefile inteiro leva segundos e o ``sjoin`` inicial leva mais.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)

SHAPEFILE_ENV = os.environ.get("CLIMATE_RISK_SHAPEFILE_MUN_PATH")

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not SHAPEFILE_ENV or not Path(SHAPEFILE_ENV).exists(),
        reason="Shapefile IBGE real não configurado via CLIMATE_RISK_SHAPEFILE_MUN_PATH.",
    ),
]


@pytest.fixture(scope="module")
def shapefile_real() -> Path:
    assert SHAPEFILE_ENV is not None
    return Path(SHAPEFILE_ENV)


def test_abre_shapefile_ibge_real(shapefile_real: Path, tmp_path: Path) -> None:
    """Sanity check: shapefile IBGE tem > 5000 municípios (BR tem ~5570)."""
    agreg = AgregadorMunicipiosGeopandas(shapefile_real, tmp_path)
    assert len(agreg._gdf_municipios) > 5000


def test_agrega_datararray_pequeno_sobre_shapefile_real(
    shapefile_real: Path, tmp_path: Path
) -> None:
    """Grade 3x3 cobrindo São Paulo capital deve conter o município 3550308."""
    agreg = AgregadorMunicipiosGeopandas(shapefile_real, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([-23.65, -23.55, -23.45])
    lon = np.array([-46.75, -46.63, -46.51])
    valores = np.ones((2, 3, 3), dtype=np.float64) * 5.0
    da = xr.DataArray(
        valores,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    assert "3550308" in set(df["municipio_id"])
    sp = df[df["municipio_id"] == "3550308"]
    # Pelo menos uma célula cai em SP com valor 5.0 — a média é 5.0.
    assert (sp["valor"] == 5.0).all()
