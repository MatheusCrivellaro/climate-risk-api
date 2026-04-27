"""Testes e2e do mount ``/estudo/`` (Slice 16, atualizado na 20.2).

Cobre o serving estático da interface HTML/CSS/JS puro e a garantia de
que o mount não interfere com ``/api/*``, ``/docs`` ou ``/app/``. A partir
da Slice 20.2 a página tem abas, modal de browser de pastas e botões de
download — os asserts validam essa estrutura.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_estudo_index_retorna_html(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    assert resposta.headers["content-type"].startswith("text/html")
    assert "Estresse Hídrico" in resposta.text


@pytest.mark.asyncio
async def test_estudo_index_tem_seis_inputs_de_pasta(cliente_api: AsyncClient) -> None:
    """Slice 17: 3 variaveis x 2 cenarios = 6 inputs de pasta."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    for cenario in ("rcp45", "rcp85"):
        for variavel in ("pr", "tas", "evap"):
            id_esperado = f"{cenario}-pasta-{variavel}"
            assert f'id="{id_esperado}"' in html, f"input {id_esperado} não encontrado"


@pytest.mark.asyncio
async def test_estudo_index_tem_abas(cliente_api: AsyncClient) -> None:
    """Slice 20.2: estrutura com 2 abas (nova execução / resultados)."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    assert 'data-tab="nova"' in html
    assert 'data-tab="resultados"' in html


@pytest.mark.asyncio
async def test_estudo_index_tem_modal_browser_de_pastas(cliente_api: AsyncClient) -> None:
    """Slice 20.2: modal nativo <dialog> para selecionar pastas."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    assert '<dialog id="modal-browser-pastas"' in html


@pytest.mark.asyncio
async def test_estudo_index_tem_botoes_procurar(cliente_api: AsyncClient) -> None:
    """Slice 20.2: cada um dos 6 inputs de pasta tem um botão Procurar."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    assert html.count('class="btn btn-secondary btn-procurar"') == 6


@pytest.mark.asyncio
async def test_estudo_index_tem_botoes_de_export(cliente_api: AsyncClient) -> None:
    """Slice 20.2: botões CSV/XLSX/JSON ligados ao endpoint de export."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    for formato in ("csv", "xlsx", "json"):
        assert f'data-formato="{formato}"' in html, f"botão {formato} não encontrado"


@pytest.mark.asyncio
async def test_estudo_serve_estilos_css(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/estilos.css")
    assert resposta.status_code == 200
    assert resposta.headers["content-type"].startswith("text/css")
    css = resposta.text
    # Variáveis principais introduzidas na Slice 20.2.
    assert "--cor-accent" in css
    assert "--cor-rcp45" in css
    assert "--cor-rcp85" in css


@pytest.mark.asyncio
async def test_estudo_serve_app_js(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/app.js")
    assert resposta.status_code == 200
    ctype = resposta.headers["content-type"]
    assert ctype.startswith("text/javascript") or ctype.startswith("application/javascript")
    assert "criarExecucoes" in resposta.text


@pytest.mark.asyncio
async def test_estudo_arquivo_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/nao-existe.xyz")
    assert resposta.status_code == 404


@pytest.mark.asyncio
async def test_estudo_nao_interfere_com_api(cliente_api: AsyncClient) -> None:
    """Mount de /estudo/ não deve sobrepor rotas /api/*."""
    resposta = await cliente_api.get("/api/health")
    assert resposta.status_code == 200


@pytest.mark.asyncio
async def test_estudo_nao_interfere_com_docs(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/docs")
    assert resposta.status_code == 200
    assert resposta.headers["content-type"].startswith("text/html")
