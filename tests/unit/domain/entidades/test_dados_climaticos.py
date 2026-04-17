"""Testes unitários do dataclass :class:`DadosClimaticos`."""

from __future__ import annotations

import dataclasses

import numpy as np
import pytest

from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos


def _fazer() -> DadosClimaticos:
    return DadosClimaticos(
        dados_diarios=np.zeros((2, 3, 3), dtype=np.float32),
        lat_2d=np.zeros((3, 3), dtype=np.float64),
        lon_2d=np.zeros((3, 3), dtype=np.float64),
        anos=np.array([2026, 2027], dtype=np.int64),
        cenario="rcp45",
        variavel="pr",
        unidade_original="kg m-2 s-1",
        conversao_unidade_aplicada=True,
        calendario="standard",
        arquivo_origem="/dados/fake.nc",
    )


def test_criacao_com_todos_os_campos() -> None:
    dados = _fazer()
    assert dados.cenario == "rcp45"
    assert dados.variavel == "pr"
    assert dados.unidade_original == "kg m-2 s-1"
    assert dados.conversao_unidade_aplicada is True
    assert dados.calendario == "standard"
    assert dados.arquivo_origem == "/dados/fake.nc"
    assert dados.dados_diarios.shape == (2, 3, 3)
    assert dados.lat_2d.shape == (3, 3)
    assert dados.lon_2d.shape == (3, 3)
    assert dados.anos.tolist() == [2026, 2027]


def test_imutabilidade_frozen() -> None:
    dados = _fazer()
    with pytest.raises(dataclasses.FrozenInstanceError):
        dados.cenario = "ssp370"  # type: ignore[misc]


def test_equality_baseada_nos_campos() -> None:
    a = _fazer()
    b = _fazer()
    # arrays numpy não suportam ``==`` em dataclass.__eq__ direto
    # (retornam arrays). Verificamos campo a campo para evitar ambiguidade.
    assert a.cenario == b.cenario
    assert a.variavel == b.variavel
    assert a.unidade_original == b.unidade_original
    assert a.conversao_unidade_aplicada == b.conversao_unidade_aplicada
    assert a.calendario == b.calendario
    assert a.arquivo_origem == b.arquivo_origem
    np.testing.assert_array_equal(a.dados_diarios, b.dados_diarios)
    np.testing.assert_array_equal(a.lat_2d, b.lat_2d)
    np.testing.assert_array_equal(a.lon_2d, b.lon_2d)
    np.testing.assert_array_equal(a.anos, b.anos)
