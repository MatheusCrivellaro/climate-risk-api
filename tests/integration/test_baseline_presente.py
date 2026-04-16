"""Garante que a baseline sintética foi commitada no repositório."""

from __future__ import annotations

from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent.parent
DIR_BASELINES = RAIZ / "tests" / "fixtures" / "baselines" / "sintetica"

ARQUIVOS_BASELINE = (
    "baseline_grade_basico.csv",
    "baseline_grade_cftime.csv",
    "baseline_pontos_basico.csv",
    "baseline_pontos_cftime.csv",
)


@pytest.mark.parametrize("nome", ARQUIVOS_BASELINE)
def test_baseline_existe_e_nao_esta_vazia(nome: str) -> None:
    caminho = DIR_BASELINES / nome
    assert caminho.exists(), f"Baseline ausente: {caminho}"
    assert caminho.stat().st_size > 0, f"Baseline vazia: {caminho}"
