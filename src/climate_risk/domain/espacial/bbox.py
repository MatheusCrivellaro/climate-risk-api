"""Caixa delimitadora espacial (bounding box) e máscara associada.

Portado de ``legacy/gera_pontos_fornecedores.py`` (função ``build_in_bbox_mask``)
com a tupla de coordenadas substituída por uma dataclass frozen.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from climate_risk.domain.espacial.longitude import ensure_lon_negpos180


@dataclass(frozen=True)
class BoundingBox:
    """Retângulo espacial em coordenadas geográficas.

    Longitudes devem estar em -180..180. O construtor valida
    ``lat_min <= lat_max``; ``lon_min <= lon_max`` não é exigido porque
    uma bbox pode cruzar o antimeridiano.
    """

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    def __post_init__(self) -> None:
        if self.lat_min > self.lat_max:
            raise ValueError(
                "BoundingBox inválida: lat_min deve ser <= lat_max "
                f"(recebido lat_min={self.lat_min}, lat_max={self.lat_max})."
            )


def mascara_bbox(lat2d: np.ndarray, lon2d: np.ndarray, bbox: BoundingBox) -> np.ndarray:
    """Retorna máscara booleana 1D (após flatten) dos pontos dentro de ``bbox``.

    Longitudes da grade são normalizadas para -180..180 antes da comparação.
    """
    lat_flat = lat2d.reshape(-1)
    lon_flat = ensure_lon_negpos180(lon2d.reshape(-1))
    return (
        (lat_flat >= bbox.lat_min)
        & (lat_flat <= bbox.lat_max)
        & (lon_flat >= bbox.lon_min)
        & (lon_flat <= bbox.lon_max)
    )
