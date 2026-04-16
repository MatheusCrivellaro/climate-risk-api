"""Testes de ``domain/espacial/bbox.py``."""

from __future__ import annotations

import numpy as np
import pytest

from climate_risk.domain.espacial.bbox import BoundingBox, mascara_bbox
from climate_risk.domain.espacial.grade import coords_to_2d


def test_bounding_box_valido() -> None:
    bbox = BoundingBox(lat_min=-23.0, lat_max=-20.0, lon_min=-47.0, lon_max=-44.0)
    assert bbox.lat_min < bbox.lat_max


def test_bounding_box_invalido_lat_min_maior_que_max() -> None:
    with pytest.raises(ValueError, match="lat_min"):
        BoundingBox(lat_min=10.0, lat_max=-10.0, lon_min=-50.0, lon_max=-40.0)


def test_mascara_bbox_grade_4x4() -> None:
    lat = np.array([-3.0, -1.0, 1.0, 3.0])
    lon = np.array([-4.0, -2.0, 0.0, 2.0])
    lat2d, lon2d = coords_to_2d(lat, lon)

    bbox = BoundingBox(lat_min=-1.5, lat_max=1.5, lon_min=-2.5, lon_max=0.5)
    mask = mascara_bbox(lat2d, lon2d, bbox)

    # Linhas esperadas (flatten linha-a-linha):
    # lat=-1: (-4,-2,0,2) -> dentro em (-2,0) -> [F,T,T,F]
    # lat= 1: (-4,-2,0,2) -> dentro em (-2,0) -> [F,T,T,F]
    esperado = np.array(
        [
            False,
            False,
            False,
            False,
            False,
            True,
            True,
            False,
            False,
            True,
            True,
            False,
            False,
            False,
            False,
            False,
        ]
    )
    np.testing.assert_array_equal(mask, esperado)


def test_mascara_bbox_normaliza_longitudes() -> None:
    lat = np.array([0.0, 0.0])
    lon_0_360 = np.array([350.0, 10.0])  # normalizadas: [-10, 10]
    lat2d, lon2d = coords_to_2d(lat, lon_0_360)

    bbox = BoundingBox(lat_min=-1.0, lat_max=1.0, lon_min=-15.0, lon_max=-5.0)
    mask = mascara_bbox(lat2d, lon2d, bbox)

    # Grade meshgrid produz (2 x 2) — apenas a coluna que bate com lon=-10 fica True.
    assert mask.reshape(2, 2)[:, 0].all()
    assert not mask.reshape(2, 2)[:, 1].any()
