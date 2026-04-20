"""Distância great-circle (Haversine) em quilômetros.

Funções puras (apenas :mod:`math` da stdlib) usadas pelo caso de uso
:class:`ConsultarResultados` (Slice 11) para filtrar resultados por raio
em torno de um ponto. O repositório aplica um BBOX aproximado como
pre-filter em SQL (barato); o caso de uso filtra pelo raio exato em
Python sobre o subconjunto devolvido.
"""

from __future__ import annotations

import math

_RAIO_TERRA_KM = 6371.0
_GRAUS_POR_KM_LAT = 1.0 / 111.0


def distancia_haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância great-circle entre dois pontos em km.

    Usa o raio médio da Terra de 6371 km. Aceita coordenadas em graus
    decimais no intervalo usual (``lat ∈ [-90, 90]``, ``lon ∈ [-180, 180]``).
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * _RAIO_TERRA_KM * math.asin(math.sqrt(a))


def bbox_aproximado_por_raio(
    lat: float, lon: float, raio_km: float
) -> tuple[float, float, float, float]:
    """BBOX que contém o círculo de raio ``raio_km`` em torno de ``(lat, lon)``.

    Aproximação útil para pré-filtrar candidatos em SQL antes do cálculo
    Haversine exato. Usa ``1° lat ≈ 111 km`` e ``1° lon ≈ 111·cos(lat) km``.
    Próximo aos polos (``|lat| ≥ ~89.4°``) o cos é saturado em 0.01 para
    evitar divisão por zero — o BBOX fica muito largo em longitude, o que
    é conservador (nenhum candidato é descartado incorretamente).

    Returns:
        Tupla ``(lat_min, lat_max, lon_min, lon_max)`` em graus decimais.
        As longitudes **não** são normalizadas para ``[-180, 180]`` — o
        caller pode detectar cruzamento do antimeridiano inspecionando
        ``lon_min < -180`` ou ``lon_max > 180``.
    """
    delta_lat = raio_km * _GRAUS_POR_KM_LAT
    cos_lat = max(math.cos(math.radians(lat)), 0.01)
    delta_lon = raio_km / (111.0 * cos_lat)
    return (lat - delta_lat, lat + delta_lat, lon - delta_lon, lon + delta_lon)
