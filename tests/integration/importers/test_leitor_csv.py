"""Testes do leitor CSV de fornecedores (Slice 10)."""

from __future__ import annotations

from climate_risk.infrastructure.importers import ler_fornecedores_csv


def test_le_csv_utf8_com_virgula() -> None:
    conteudo = b"nome,cidade,uf\nAcme,Sao Paulo,SP\nBeta,Rio,RJ\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 2
    assert linhas[0].nome == "Acme"
    assert linhas[0].cidade == "Sao Paulo"
    assert linhas[0].uf == "SP"
    assert linhas[0].numero_linha == 2
    assert linhas[1].numero_linha == 3


def test_le_csv_com_ponto_e_virgula() -> None:
    conteudo = b"nome;cidade;uf\nAcme;Sao Paulo;SP\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 1
    assert linhas[0].cidade == "Sao Paulo"


def test_le_csv_latin1_fallback() -> None:
    conteudo = "nome,cidade,uf\nAção,São Paulo,SP\n".encode("latin-1")

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 1
    assert linhas[0].nome == "Ação"


def test_le_csv_utf8_com_bom() -> None:
    conteudo = b"\xef\xbb\xbfnome,cidade,uf\nAcme,Sao Paulo,SP\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 1
    assert linhas[0].nome == "Acme"


def test_le_csv_aceita_aliases_cabecalho() -> None:
    conteudo = b"razao_social,municipio,estado\nAcme,Sao Paulo,SP\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 1
    assert linhas[0].nome == "Acme"
    assert linhas[0].cidade == "Sao Paulo"
    assert linhas[0].uf == "SP"


def test_le_csv_case_insensitive_no_cabecalho() -> None:
    conteudo = b"Nome,Cidade,UF\nAcme,SP,SP\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 1


def test_csv_vazio_retorna_lista_vazia() -> None:
    assert ler_fornecedores_csv(b"") == []


def test_csv_sem_cabecalho_obrigatorio_retorna_vazio() -> None:
    conteudo = b"outro,campo\nvalor1,valor2\n"

    assert ler_fornecedores_csv(conteudo) == []


def test_csv_ignora_linhas_totalmente_vazias() -> None:
    conteudo = b"nome,cidade,uf\nAcme,SP,SP\n,,\nBeta,RJ,RJ\n"

    linhas = ler_fornecedores_csv(conteudo)

    assert len(linhas) == 2
    assert [linha.numero_linha for linha in linhas] == [2, 4]
