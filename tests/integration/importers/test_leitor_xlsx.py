"""Testes do leitor XLSX de fornecedores (Slice 10)."""

from __future__ import annotations

import io

import openpyxl

from climate_risk.infrastructure.importers import ler_fornecedores_xlsx


def _xlsx_bytes(linhas: list[list[object]]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None
    for linha in linhas:
        ws.append(linha)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_le_xlsx_basico() -> None:
    conteudo = _xlsx_bytes(
        [
            ["nome", "cidade", "uf"],
            ["Acme", "São Paulo", "SP"],
            ["Beta", "Rio", "RJ"],
        ]
    )

    linhas = ler_fornecedores_xlsx(conteudo)

    assert len(linhas) == 2
    assert linhas[0].nome == "Acme"
    assert linhas[0].cidade == "São Paulo"
    assert linhas[0].numero_linha == 2
    assert linhas[1].numero_linha == 3


def test_le_xlsx_aceita_aliases() -> None:
    conteudo = _xlsx_bytes(
        [
            ["razao_social", "municipio", "estado"],
            ["Acme", "São Paulo", "SP"],
        ]
    )

    linhas = ler_fornecedores_xlsx(conteudo)

    assert len(linhas) == 1
    assert linhas[0].nome == "Acme"
    assert linhas[0].uf == "SP"


def test_le_xlsx_ignora_linhas_totalmente_vazias() -> None:
    conteudo = _xlsx_bytes(
        [
            ["nome", "cidade", "uf"],
            ["Acme", "SP", "SP"],
            [None, None, None],
            ["Beta", "RJ", "RJ"],
        ]
    )

    linhas = ler_fornecedores_xlsx(conteudo)

    assert len(linhas) == 2
    assert [linha.numero_linha for linha in linhas] == [2, 4]


def test_le_xlsx_cabecalho_ausente_retorna_vazio() -> None:
    conteudo = _xlsx_bytes(
        [
            ["outro_campo", "mais_um"],
            ["valor1", "valor2"],
        ]
    )

    assert ler_fornecedores_xlsx(conteudo) == []


def test_le_xlsx_converte_celulas_nao_string() -> None:
    conteudo = _xlsx_bytes(
        [
            ["nome", "cidade", "uf"],
            [123, "SP", "SP"],
        ]
    )

    linhas = ler_fornecedores_xlsx(conteudo)

    assert len(linhas) == 1
    assert linhas[0].nome == "123"
