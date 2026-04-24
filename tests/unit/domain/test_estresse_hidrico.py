"""Testes dos cálculos puros de estresse hídrico (Slice 13).

Unitários e sem I/O — validam a aritmética, o tratamento de ``NaN`` e o
uso dos limiares configurados. O teste de regressão numérica lê o arquivo
``esperado.json`` gerado por ``tests/fixtures/climatologia_multi/gerar_fixtures.py``
e confere que a função reproduz os valores calculados à mão.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from climate_risk.domain.calculos.estresse_hidrico import (
    IndicesAnuaisEstresseHidrico,
    ParametrosIndicesEstresseHidrico,
    calcular_deficit_hidrico_diario,
    calcular_dias_secos_quentes,
    calcular_indices_anuais_estresse_hidrico,
)

BASELINE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "baselines"
    / "estresse_hidrico_sintetico"
    / "esperado.json"
)


def _params_default() -> ParametrosIndicesEstresseHidrico:
    return ParametrosIndicesEstresseHidrico()


def test_dias_secos_quentes_conta_corretamente() -> None:
    pr = np.array([0.0, 0.5, 1.0, 2.0, 0.8, 0.0, 1.2, 0.9, 0.3, 5.0])
    tas = np.array([31.0, 32.0, 29.0, 33.0, 30.5, 28.0, 31.5, 34.0, 25.0, 32.0])
    evap = np.array([3.0, 4.0, 2.0, 1.0, 5.0, 2.5, 3.0, 2.0, 1.5, 0.5])

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    # Seco (pr<=1.0): [0, 1, 2, 4, 5, 7, 8]
    # Quente (tas>=30.0): [0, 1, 3, 4, 6, 7, 9]
    # Interseção: [0, 1, 4, 7] → 4 dias.
    assert resultado.dias_secos_quentes == 4


def test_intensidade_soma_deficit_nos_dias_secos_quentes() -> None:
    pr = np.array([0.0, 0.5, 2.0])
    tas = np.array([31.0, 30.5, 33.0])  # todos quentes
    evap = np.array([5.0, 4.0, 10.0])  # deficit: 5, 3.5, 8 — mas apenas 2 secos

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    assert resultado.dias_secos_quentes == 2
    assert resultado.intensidade_estresse == pytest.approx(5.0 + 3.5)
    assert resultado.deficit_total_mm == pytest.approx(5.0 + 3.5 + 8.0)


def test_deficit_total_inclui_dias_chuvosos_com_deficit_negativo() -> None:
    pr = np.array([10.0, 0.0])
    tas = np.array([20.0, 31.0])  # só o dia 1 é quente; dia 0 é chuvoso
    evap = np.array([1.0, 0.0])  # deficit dia 0: -9; dia 1: 0

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    assert resultado.dias_secos_quentes == 1
    # Dia 1 não entra em intensidade pois deficit == 0 (evap - pr = 0), mas é contado.
    assert resultado.intensidade_estresse == pytest.approx(0.0)
    # Déficit total: (1-10) + (0-0) = -9.0
    assert resultado.deficit_total_mm == pytest.approx(-9.0)


def test_limiares_customizados_alteram_contagem() -> None:
    pr = np.array([0.5, 0.5, 0.5])
    tas = np.array([28.0, 29.5, 30.5])
    evap = np.array([1.0, 1.0, 1.0])

    # Com default (tas>=30), apenas 1 dia.
    r_default = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())
    assert r_default.dias_secos_quentes == 1

    # Baixando o limiar de temperatura para 29, entram 2 dias.
    r_custom = calcular_indices_anuais_estresse_hidrico(
        pr,
        tas,
        evap,
        ParametrosIndicesEstresseHidrico(limiar_pr_mm_dia=1.0, limiar_tas_c=29.0),
    )
    assert r_custom.dias_secos_quentes == 2


def test_nan_em_qualquer_variavel_descarta_dia() -> None:
    pr = np.array([0.5, 0.5, np.nan, 0.5])
    tas = np.array([31.0, np.nan, 31.0, 31.0])
    evap = np.array([2.0, 2.0, 2.0, np.nan])

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    # Apenas o dia 0 é totalmente válido e cumpre os critérios.
    assert resultado.dias_secos_quentes == 1
    assert resultado.intensidade_estresse == pytest.approx(1.5)
    assert resultado.deficit_total_mm == pytest.approx(1.5)


def test_ano_sem_secos_quentes_retorna_zero_e_nao_nan() -> None:
    pr = np.array([5.0, 10.0, 15.0])
    tas = np.array([25.0, 22.0, 20.0])  # nenhum quente
    evap = np.array([1.0, 2.0, 3.0])

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    assert resultado.dias_secos_quentes == 0
    assert resultado.intensidade_estresse == 0.0  # não NaN
    assert resultado.deficit_total_mm == pytest.approx((1.0 - 5.0) + (2.0 - 10.0) + (3.0 - 15.0))


def test_ano_todo_nan_retorna_zeros() -> None:
    pr = np.full(5, np.nan)
    tas = np.full(5, np.nan)
    evap = np.full(5, np.nan)

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    assert resultado == IndicesAnuaisEstresseHidrico(
        dias_secos_quentes=0,
        intensidade_estresse=0.0,
        deficit_total_mm=0.0,
    )


def test_calcular_dias_secos_quentes_retorna_mascara_booleana() -> None:
    pr = np.array([0.5, 2.0, 0.0])
    tas = np.array([31.0, 31.0, 25.0])
    mascara = calcular_dias_secos_quentes(pr, tas, _params_default())
    assert mascara.dtype == np.bool_
    np.testing.assert_array_equal(mascara, [True, False, False])


def test_calcular_deficit_hidrico_diario_retorna_evap_menos_pr() -> None:
    pr = np.array([1.0, 2.0, 3.0])
    evap = np.array([4.0, 5.0, 6.0])
    deficit = calcular_deficit_hidrico_diario(evap, pr)
    np.testing.assert_array_equal(deficit, [3.0, 3.0, 3.0])


def test_regressao_baseline_sintetica() -> None:
    """Reproduz exatamente o ``esperado.json`` gerado pelo script de fixtures."""
    esperado = json.loads(BASELINE.read_text())

    pr = np.array([0.5, 0.2, 5.0, 0.0, 0.1, 2.0, 0.3, 0.0, 10.0, 0.4])
    tas = np.array([28.0, 32.0, 31.5, 25.0, 33.0, 29.0, 35.0, 27.0, 30.5, 24.0])
    evap = np.array([3.0, 4.5, 2.0, 1.0, 5.0, 2.5, 6.0, 1.5, 4.0, 0.8])

    resultado = calcular_indices_anuais_estresse_hidrico(pr, tas, evap, _params_default())

    assert resultado.dias_secos_quentes == esperado["dias_secos_quentes"]
    assert resultado.intensidade_estresse == pytest.approx(esperado["intensidade_estresse"])
    assert resultado.deficit_total_mm == pytest.approx(esperado["deficit_total_mm"])
