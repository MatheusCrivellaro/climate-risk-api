"""Testes de ``domain/espacial/longitude.py``."""

from __future__ import annotations

import numpy as np
import pytest

from climate_risk.domain.espacial.longitude import ensure_lon_negpos180, normalize_lon


def test_ensure_lon_negpos180_converte_0_a_360() -> None:
    entrada = np.array([0.0, 90.0, 180.0, 270.0, 360.0])
    esperado = np.array([0.0, 90.0, -180.0, -90.0, 0.0])
    np.testing.assert_allclose(ensure_lon_negpos180(entrada), esperado)


def test_ensure_lon_negpos180_preserva_negativos() -> None:
    entrada = np.array([-45.0, -180.0, -90.0])
    np.testing.assert_allclose(ensure_lon_negpos180(entrada), entrada)


def test_ensure_lon_negpos180_idempotente() -> None:
    entrada = np.array([-179.9, -45.0, 0.0, 45.0, 179.9, 200.0, 350.0])
    uma_vez = ensure_lon_negpos180(entrada)
    duas_vezes = ensure_lon_negpos180(uma_vez)
    np.testing.assert_allclose(uma_vez, duas_vezes)


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        (270.0, -90.0),
        (90.0, 90.0),
        (-45.0, -45.0),
        (360.0, 0.0),
        (180.0, -180.0),
    ],
)
def test_normalize_lon_escalar(entrada: float, esperado: float) -> None:
    assert normalize_lon(entrada) == pytest.approx(esperado)


def test_normalize_lon_idempotente() -> None:
    valor = 320.5
    primeiro = normalize_lon(valor)
    segundo = normalize_lon(primeiro)
    assert primeiro == pytest.approx(segundo)
