"""Testes e2e do mount ``/estudo/`` (Slice 16).

Cobre o serving estático da interface HTML/CSS/JS puro e a garantia de
que o mount não interfere com ``/api/*``, ``/docs`` ou ``/app/``.
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
async def test_estudo_index_tem_seis_campos_de_pasta(cliente_api: AsyncClient) -> None:
    """Slice 17: a página deve ter os 6 campos (3 vars x 2 cenarios)."""
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200
    html = resposta.text
    for cenario in ("rcp45", "rcp85"):
        for var in ("pr", "tas", "evap"):
            assert f'id="{cenario}-pasta-{var}"' in html, (
                f"campo {cenario}-pasta-{var} não encontrado no HTML"
            )


@pytest.mark.asyncio
async def test_estudo_serve_estilos_css(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/estilos.css")
    assert resposta.status_code == 200
    assert resposta.headers["content-type"].startswith("text/css")
    assert "--cor-primaria" in resposta.text


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
