"""Testes de :func:`parsear_lista_legacy`."""

from __future__ import annotations

import pytest

from climate_risk.application.cobertura import FornecedorEntrada, parsear_lista_legacy


def test_parsear_linhas_validas() -> None:
    texto = "MONTE BELO/MG\nARACAJU/SE"
    resultado = parsear_lista_legacy(texto)
    assert resultado == [
        FornecedorEntrada(identificador="MONTE BELO/MG", cidade="MONTE BELO", uf="MG"),
        FornecedorEntrada(identificador="ARACAJU/SE", cidade="ARACAJU", uf="SE"),
    ]


def test_ignora_linhas_vazias() -> None:
    texto = "\n\nMONTE BELO/MG\n\n\nARACAJU/SE\n\n"
    assert len(parsear_lista_legacy(texto)) == 2


def test_ignora_linhas_sem_barra() -> None:
    texto = "MONTE BELO/MG\n# comentario sem barra\nARACAJU/SE"
    assert [f.identificador for f in parsear_lista_legacy(texto)] == [
        "MONTE BELO/MG",
        "ARACAJU/SE",
    ]


def test_uf_normalizada_para_uppercase() -> None:
    resultado = parsear_lista_legacy("são paulo/sp")
    assert resultado[0].uf == "SP"


def test_espacos_strippados() -> None:
    texto = "   MONTE BELO / MG   "
    resultado = parsear_lista_legacy(texto)
    assert resultado[0].cidade == "MONTE BELO"
    assert resultado[0].uf == "MG"


def test_texto_vazio_devolve_vazio() -> None:
    assert parsear_lista_legacy("") == []
    assert parsear_lista_legacy("   \n\n   ") == []


def test_identificador_preserva_linha_original_strippada() -> None:
    resultado = parsear_lista_legacy("   FOO/BA   ")
    assert resultado[0].identificador == "FOO/BA"


def test_cidade_vazia_ignorada() -> None:
    assert parsear_lista_legacy("/MG") == []


def test_uf_vazia_ignorada() -> None:
    assert parsear_lista_legacy("FOO/") == []


def test_cidade_com_barra_usa_rsplit() -> None:
    """``CONCEICAO DO MATO DENTRO/MG`` — rsplit preserva cidade composta."""
    resultado = parsear_lista_legacy("CONCEICAO DO MATO DENTRO/MG")
    assert resultado[0].cidade == "CONCEICAO DO MATO DENTRO"
    assert resultado[0].uf == "MG"


def test_multiplas_barras_ultima_e_uf() -> None:
    """``A/B/C`` -> cidade=``A/B``, uf=``C``."""
    resultado = parsear_lista_legacy("SÃO MIGUEL D'OESTE/SC")
    assert resultado[0].uf == "SC"


@pytest.mark.parametrize(
    ("texto", "esperados"),
    [
        ("", 0),
        ("FOO/BA", 1),
        ("FOO/BA\nBAR/RJ", 2),
        ("FOO/BA\n\nBAR/RJ", 2),
        ("linha sem barra\nFOO/BA", 1),
        ("FOO/BA\r\nBAR/RJ", 2),
    ],
)
def test_conta_linhas_validas(texto: str, esperados: int) -> None:
    assert len(parsear_lista_legacy(texto)) == esperados


def test_ordem_preservada() -> None:
    texto = "C/SP\nA/RJ\nB/MG"
    ids = [f.identificador for f in parsear_lista_legacy(texto)]
    assert ids == ["C/SP", "A/RJ", "B/MG"]


def test_linhas_apenas_com_barra() -> None:
    assert parsear_lista_legacy("/\n/\n") == []
