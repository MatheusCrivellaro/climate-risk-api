"""Utilidades de grade 2D para dados climáticos.

Portado do script legado ``gera_pontos_fornecedores.py`` conforme ADR-001
(código legado removido na Slice 12).
"""

from __future__ import annotations

import numpy as np

from climate_risk.domain.espacial.longitude import ensure_lon_negpos180, normalize_lon


def coords_to_2d(lat_vals: np.ndarray, lon_vals: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Retorna ``(lat2d, lon2d)`` no shape da grade.

    - Se ambas já forem 2D, devolve como estão.
    - Se ambas forem 1D, aplica ``meshgrid`` produzindo ``(ny, nx)``.
    - Fallback: força reshape/meshgrid para shapes inesperados.
    """
    lat_arr = np.asarray(lat_vals)
    lon_arr = np.asarray(lon_vals)
    if lat_arr.ndim == 2 and lon_arr.ndim == 2:
        return lat_arr, lon_arr
    if lat_arr.ndim == 1 and lon_arr.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon_arr, lat_arr)  # (ny, nx)
        return lat2d, lon2d
    lon2d, lat2d = np.meshgrid(np.asarray(lon_arr).reshape(-1), np.asarray(lat_arr).reshape(-1))
    return lat2d, lon2d


def indice_mais_proximo(
    lat2d: np.ndarray,
    lon2d: np.ndarray,
    lat: float,
    lon: float,
) -> tuple[int, int]:
    """Retorna ``(iy, ix)`` do pixel mais próximo a ``(lat, lon)``.

    Usa distância euclidiana em graus (adequado para resoluções CORDEX
    típicas de ~25 km). Longitudes são normalizadas para -180..180 antes
    da comparação.
    """
    latf = lat2d.reshape(-1)
    lonf = ensure_lon_negpos180(lon2d.reshape(-1))
    target_lon = normalize_lon(lon)
    target_lat = float(lat)

    dist = (latf - target_lat) ** 2 + (lonf - target_lon) ** 2
    k = int(np.argmin(dist))
    _, nx = lat2d.shape
    iy, ix = divmod(k, nx)
    return iy, ix
