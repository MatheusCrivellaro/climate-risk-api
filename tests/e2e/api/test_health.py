"""Teste smoke da rota /health."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from climate_risk.interfaces.app import create_app


@pytest.mark.asyncio
async def test_health_retorna_ok() -> None:
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        resposta = await client.get("/api/health")

    assert resposta.status_code == 200
    assert resposta.json() == {"status": "ok"}
    assert "X-Correlation-ID" in resposta.headers
