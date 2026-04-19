"""Adaptador :class:`CalculadorCentroide` baseado em ``shapely``.

Usa :meth:`shapely.geometry.base.BaseGeometry.representative_point` — mais
robusto que ``centroid`` para multipolígonos com ilhas ou recortes
estranhos (municípios costeiros, áreas litorâneas). O ponto retornado é
*sempre* interno à geometria.
"""

from __future__ import annotations

from typing import Any

from shapely.geometry import shape
from shapely.ops import unary_union

from climate_risk.domain.excecoes import ErroClienteIBGE


class CalculadorShapely:
    """Extrai ``(lat, lon)`` a partir de um documento GeoJSON do IBGE."""

    def calcular(self, geojson: dict[str, Any]) -> tuple[float, float]:
        tipo = geojson.get("type")
        if tipo == "Feature":
            geometria = shape(geojson["geometry"])
        elif tipo == "FeatureCollection":
            geometrias = [shape(f["geometry"]) for f in geojson.get("features", [])]
            if not geometrias:
                raise ErroClienteIBGE(
                    "FeatureCollection sem features",
                    endpoint="(geometria)",
                )
            geometria = unary_union(geometrias)
        elif tipo in {
            "Polygon",
            "MultiPolygon",
            "Point",
            "LineString",
            "MultiLineString",
            "MultiPoint",
        }:
            geometria = shape(geojson)
        else:
            raise ErroClienteIBGE(
                f"GeoJSON com tipo não suportado: {tipo!r}",
                endpoint="(geometria)",
            )

        ponto = geometria.representative_point()
        return float(ponto.y), float(ponto.x)
