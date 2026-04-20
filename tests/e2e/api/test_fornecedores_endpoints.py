"""Testes e2e dos endpoints ``/fornecedores`` (Slice 10)."""

from __future__ import annotations

import io

import openpyxl
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_post_cria_fornecedor_201(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/api/fornecedores",
        json={"nome": "Acme", "cidade": "São Paulo", "uf": "sp"},
    )

    assert resposta.status_code == 201, resposta.text
    corpo = resposta.json()
    assert corpo["nome"] == "Acme"
    assert corpo["cidade"] == "São Paulo"
    assert corpo["uf"] == "SP"
    assert corpo["id"].startswith("forn_")
    assert corpo["municipio_id"] is None
    assert corpo["lat"] is None and corpo["lon"] is None


@pytest.mark.asyncio
async def test_post_payload_invalido_422(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/api/fornecedores", json={"nome": "", "cidade": "SP", "uf": "SP"}
    )

    assert resposta.status_code == 422


@pytest.mark.asyncio
async def test_get_lista_paginada_com_filtro_uf(cliente_api: AsyncClient) -> None:
    for nome, uf in [("A", "SP"), ("B", "SP"), ("C", "RJ")]:
        await cliente_api.post(
            "/api/fornecedores",
            json={"nome": nome, "cidade": "Cidade", "uf": uf},
        )

    resposta = await cliente_api.get("/api/fornecedores", params={"uf": "SP", "limit": 10})

    assert resposta.status_code == 200
    pagina = resposta.json()
    assert pagina["total"] == 2
    assert pagina["limit"] == 10
    assert pagina["offset"] == 0
    assert {item["nome"] for item in pagina["itens"]} == {"A", "B"}


@pytest.mark.asyncio
async def test_get_detalhe_por_id(cliente_api: AsyncClient) -> None:
    criado = await cliente_api.post(
        "/api/fornecedores",
        json={"nome": "Acme", "cidade": "SP", "uf": "SP"},
    )
    forn_id = criado.json()["id"]

    resposta = await cliente_api.get(f"/api/fornecedores/{forn_id}")

    assert resposta.status_code == 200
    assert resposta.json()["id"] == forn_id


@pytest.mark.asyncio
async def test_get_detalhe_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/api/fornecedores/forn_nao_existe")

    assert resposta.status_code == 404
    assert "application/problem+json" in resposta.headers["content-type"]


@pytest.mark.asyncio
async def test_delete_204_e_depois_404(cliente_api: AsyncClient) -> None:
    criado = await cliente_api.post(
        "/api/fornecedores",
        json={"nome": "Acme", "cidade": "SP", "uf": "SP"},
    )
    forn_id = criado.json()["id"]

    resposta = await cliente_api.delete(f"/api/fornecedores/{forn_id}")
    assert resposta.status_code == 204

    segunda = await cliente_api.delete(f"/api/fornecedores/{forn_id}")
    assert segunda.status_code == 404


@pytest.mark.asyncio
async def test_importar_csv(cliente_api: AsyncClient) -> None:
    csv_conteudo = b"nome,cidade,uf\nAcme,Sao Paulo,SP\nBeta,Rio,RJ\n"

    resposta = await cliente_api.post(
        "/api/fornecedores/importar",
        files={"arquivo": ("fornecedores.csv", csv_conteudo, "text/csv")},
    )

    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total_linhas"] == 2
    assert corpo["importados"] == 2
    assert corpo["duplicados"] == 0
    assert corpo["erros"] == []


@pytest.mark.asyncio
async def test_importar_xlsx(cliente_api: AsyncClient) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None
    ws.append(["nome", "cidade", "uf"])
    ws.append(["Acme", "Sao Paulo", "SP"])
    ws.append(["Beta", "Rio", "RJ"])
    buf = io.BytesIO()
    wb.save(buf)

    resposta = await cliente_api.post(
        "/api/fornecedores/importar",
        files={
            "arquivo": (
                "fornecedores.xlsx",
                buf.getvalue(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )

    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["importados"] == 2


@pytest.mark.asyncio
async def test_importar_formato_invalido_400(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/api/fornecedores/importar",
        files={"arquivo": ("lixo.txt", b"qualquer coisa", "text/plain")},
    )

    assert resposta.status_code == 400
    assert "application/problem+json" in resposta.headers["content-type"]


@pytest.mark.asyncio
async def test_importar_csv_relata_erros_por_linha(cliente_api: AsyncClient) -> None:
    csv_conteudo = b"nome,cidade,uf\n,SP,SP\nAcme, ,SP\nBeta,Rio,RJX\n"

    resposta = await cliente_api.post(
        "/api/fornecedores/importar",
        files={"arquivo": ("fornecedores.csv", csv_conteudo, "text/csv")},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["importados"] == 0
    assert len(corpo["erros"]) == 3
    linhas_com_erro = {e["linha"] for e in corpo["erros"]}
    assert linhas_com_erro == {2, 3, 4}


@pytest.mark.asyncio
async def test_importar_csv_duplicados_internos_e_no_banco(
    cliente_api: AsyncClient,
) -> None:
    await cliente_api.post(
        "/api/fornecedores",
        json={"nome": "JaExiste", "cidade": "SP", "uf": "SP"},
    )

    csv_conteudo = b"nome,cidade,uf\nJaExiste,SP,SP\nNovo,SP,SP\nNovo,SP,SP\n"

    resposta = await cliente_api.post(
        "/api/fornecedores/importar",
        files={"arquivo": ("fornecedores.csv", csv_conteudo, "text/csv")},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["importados"] == 1
    assert corpo["duplicados"] == 2
