"""Testes de :class:`CalculadorShapely` (Slice 8)."""

from __future__ import annotations

import pytest

from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.infrastructure.geocodificacao.calculador_shapely import CalculadorShapely


def _quadrado(xmin: float, ymin: float, xmax: float, ymax: float) -> dict[str, object]:
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [xmin, ymin],
                [xmax, ymin],
                [xmax, ymax],
                [xmin, ymax],
                [xmin, ymin],
            ]
        ],
    }


def test_feature_simples() -> None:
    geojson = {"type": "Feature", "geometry": _quadrado(-46.5, -23.5, -46.0, -23.0)}
    lat, lon = CalculadorShapely().calcular(geojson)
    # representative_point de um retângulo simétrico fica no centro.
    assert -23.5 <= lat <= -23.0
    assert -46.5 <= lon <= -46.0


def test_feature_collection_unifica_e_retorna_ponto_interno() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": _quadrado(-46.5, -23.5, -46.0, -23.0)},
            {"type": "Feature", "geometry": _quadrado(-46.0, -23.5, -45.5, -23.0)},
        ],
    }
    lat, lon = CalculadorShapely().calcular(geojson)
    assert -23.5 <= lat <= -23.0
    assert -46.5 <= lon <= -45.5


def test_geometria_raiz_polygon() -> None:
    lat, lon = CalculadorShapely().calcular(_quadrado(-1.0, -1.0, 1.0, 1.0))
    assert -1.0 <= lat <= 1.0
    assert -1.0 <= lon <= 1.0


def test_feature_collection_vazia_erra() -> None:
    geojson: dict[str, object] = {"type": "FeatureCollection", "features": []}
    with pytest.raises(ErroClienteIBGE):
        CalculadorShapely().calcular(geojson)


def test_tipo_invalido_erra() -> None:
    with pytest.raises(ErroClienteIBGE):
        CalculadorShapely().calcular({"type": "ThisIsNotGeoJSON"})
