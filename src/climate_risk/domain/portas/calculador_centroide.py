"""Porta :class:`CalculadorCentroide` — extrai ``(lat, lon)`` de um GeoJSON.

O adaptador concreto (``infrastructure/geocodificacao/calculador_shapely.py``)
usa ``shapely.representative_point`` — mais robusto que o centroide em
multipolígonos com ilhas/recortes estranhos (ex.: municípios costeiros).
"""

from __future__ import annotations

from typing import Any, Protocol


class CalculadorCentroide(Protocol):
    """Extrai um ponto representativo em graus decimais a partir de GeoJSON."""

    def calcular(self, geojson: dict[str, Any]) -> tuple[float, float]:
        """Calcula ``(lat, lon)`` do ponto representativo.

        Args:
            geojson: ``Feature`` ou ``FeatureCollection`` (como retornado pela
                API de malhas do IBGE).

        Returns:
            Tupla ``(lat, lon)`` em graus decimais (EPSG:4326).
        """
        ...
