"""Utilidades de longitude para normalização entre convenções 0..360 e -180..180.

Portado do script legado ``gera_pontos_fornecedores.py`` conforme ADR-001
(código legado removido na Slice 12).
"""

from __future__ import annotations

import numpy as np


def ensure_lon_negpos180(lon_vals: np.ndarray) -> np.ndarray:
    """Converte longitudes do intervalo 0..360 para -180..180.

    Valores já negativos permanecem inalterados (a operação é idempotente
    para entradas já normalizadas).
    """
    return ((lon_vals + 180.0) % 360.0) - 180.0


def normalize_lon(lon: float) -> float:
    """Versão escalar de :func:`ensure_lon_negpos180`."""
    return float(((float(lon) + 180.0) % 360.0) - 180.0)
