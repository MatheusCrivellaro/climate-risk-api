"""Testes do mount do frontend em ``/app/`` (Slice 20.1).

A partir da Slice 20, o frontend React em ``/app/`` foi desativado — o
backend não monta mais nada ali. Estes testes garantem que ``/app/*``
retorna 404 e que o mount de ``/estudo/`` continua intacto.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_app_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/app")
    assert resposta.status_code == 404


@pytest.mark.asyncio
async def test_rota_aninhada_em_app_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/app/execucoes/123")
    assert resposta.status_code == 404


@pytest.mark.asyncio
async def test_assets_de_app_retornam_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/app/assets/app.js")
    assert resposta.status_code == 404


@pytest.mark.asyncio
async def test_estudo_continua_acessivel(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/estudo/")
    assert resposta.status_code == 200


@pytest.mark.asyncio
async def test_api_continua_acessivel(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/api/health")
    assert resposta.status_code == 200
    assert resposta.json() == {"status": "ok"}
