"""Testes de :func:`normalizar_nome_municipio` (Slice 8)."""

from __future__ import annotations

import pytest

from climate_risk.domain.util.normalizacao import normalizar_nome_municipio


@pytest.mark.parametrize(
    "entrada,esperado",
    [
        # Casefold básico.
        ("São Paulo", "sao paulo"),
        ("FLORIANÓPOLIS", "florianopolis"),
        ("rio de janeiro", "rio de janeiro"),
        # Acentos variados.
        ("Ção-Ção", "cao cao"),
        ("Ñoño", "nono"),
        ("Vitória", "vitoria"),
        # Regra d'oeste em combinações conhecidas.
        ("Alta Floresta D'Oeste", "alta floresta doeste"),
        ("Alvorada d'Oeste", "alvorada doeste"),
        ("Espigão D'OESTE", "espigao doeste"),
        # Apóstrofos tipográficos (unicode 2018/2019).
        ("D\u2019\u00c1vila", "davila"),
        ("Santa B\u00e1rbara d\u2019Oeste", "santa barbara doeste"),
        # Hífen vira espaço.
        ("São João del-Rei", "sao joao del rei"),
        ("Mogi-Mirim", "mogi mirim"),
        # Espaços múltiplos + strip.
        ("  Curitiba  ", "curitiba"),
        ("Belo  Horizonte", "belo horizonte"),
        # Edge cases: vazio e só espaço.
        ("", ""),
        ("   ", ""),
    ],
)
def test_normalizar_nome_municipio(entrada: str, esperado: str) -> None:
    assert normalizar_nome_municipio(entrada) == esperado


def test_idempotente() -> None:
    """Aplicar duas vezes produz o mesmo resultado — chave estável p/ cache."""
    bruto = "São João del-Rei"
    uma_vez = normalizar_nome_municipio(bruto)
    duas_vezes = normalizar_nome_municipio(uma_vez)
    assert uma_vez == duas_vezes == "sao joao del rei"
