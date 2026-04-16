"""Testes de ``domain/espacial/grade.py``."""

from __future__ import annotations

import numpy as np

from climate_risk.domain.espacial.grade import coords_to_2d, indice_mais_proximo


def test_coords_to_2d_com_entrada_1d() -> None:
    lat = np.array([-23.0, -22.0, -21.0])
    lon = np.array([-47.0, -46.0, -45.0, -44.0])
    lat2d, lon2d = coords_to_2d(lat, lon)
    assert lat2d.shape == (3, 4)
    assert lon2d.shape == (3, 4)
    # Cada linha de lat2d tem o mesmo valor de latitude; cada coluna de lon2d, o mesmo valor de lon.
    np.testing.assert_allclose(lat2d[:, 0], lat)
    np.testing.assert_allclose(lon2d[0, :], lon)


def test_coords_to_2d_com_entrada_2d_retorna_identico() -> None:
    lon1d = np.array([-47.0, -46.0])
    lat1d = np.array([-23.0, -22.0])
    lon2d, lat2d = np.meshgrid(lon1d, lat1d)
    out_lat, out_lon = coords_to_2d(lat2d, lon2d)
    assert out_lat is lat2d or np.array_equal(out_lat, lat2d)
    assert out_lon is lon2d or np.array_equal(out_lon, lon2d)


def test_indice_mais_proximo_ponto_conhecido() -> None:
    lat = np.linspace(-23.0, -20.0, 4)  # [-23, -22, -21, -20]
    lon = np.linspace(-47.0, -44.0, 4)  # [-47, -46, -45, -44]
    lat2d, lon2d = coords_to_2d(lat, lon)

    iy, ix = indice_mais_proximo(lat2d, lon2d, lat=-22.1, lon=-45.9)
    assert (iy, ix) == (1, 1)  # (-22, -46) é o mais próximo


def test_indice_mais_proximo_aceita_longitude_0_360() -> None:
    lat = np.linspace(-23.0, -20.0, 4)
    lon = np.linspace(-47.0, -44.0, 4)
    lat2d, lon2d = coords_to_2d(lat, lon)

    # 314.0 graus equivale a -46.0 após normalização.
    iy, ix = indice_mais_proximo(lat2d, lon2d, lat=-22.0, lon=314.0)
    assert (iy, ix) == (1, 1)


def test_indice_mais_proximo_com_grade_0_360_na_grade() -> None:
    lat = np.linspace(-5.0, 5.0, 3)
    lon_0_360 = np.array([350.0, 355.0, 0.0, 5.0])  # equivale a [-10, -5, 0, 5]
    lat2d, lon2d = coords_to_2d(lat, lon_0_360)

    iy, ix = indice_mais_proximo(lat2d, lon2d, lat=0.0, lon=-4.5)
    # após normalização da grade: [-10, -5, 0, 5]; alvo -4.5 fica mais perto de -5.
    assert ix == 1
    assert iy == 1
