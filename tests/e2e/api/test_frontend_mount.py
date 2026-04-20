"""Testes do mount do frontend em ``/app/``.

Cobre os dois cenários possíveis:

- Sem ``frontend/dist/`` presente: o backend responde 503 com uma
  mensagem HTML explicando como rodar o build.
- Com build presente (simulado por diretório temporário): qualquer rota
  sob ``/app/`` devolve o ``index.html`` para que o React Router cuide
  do roteamento client-side.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from climate_risk.interfaces import app as app_module


@pytest.mark.asyncio
async def test_sem_build_retorna_503_com_mensagem(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Aponta FRONTEND_DIST para um caminho inexistente.
    monkeypatch.setattr(app_module, "FRONTEND_DIST", tmp_path / "nao-existe")
    api = app_module.create_app()

    transport = ASGITransport(app=api)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        resposta = await client.get("/app")
        resposta_aninhada = await client.get("/app/execucoes/123")

    assert resposta.status_code == 503
    assert "Frontend não disponível" in resposta.text
    assert resposta_aninhada.status_code == 503


@pytest.mark.asyncio
async def test_com_build_serve_index_para_qualquer_rota_spa(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(
        "<!doctype html><html><body id='spa'>ok</body></html>", encoding="utf-8"
    )
    (dist / "assets" / "app.js").write_text("console.log('hi');", encoding="utf-8")

    monkeypatch.setattr(app_module, "FRONTEND_DIST", dist)
    api = app_module.create_app()

    transport = ASGITransport(app=api)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        raiz = await client.get("/app")
        rota_spa = await client.get("/app/qualquer/rota/inventada")
        asset = await client.get("/app/assets/app.js")

    assert raiz.status_code == 200
    assert raiz.headers["content-type"].startswith("text/html")
    assert "spa" in raiz.text

    assert rota_spa.status_code == 200
    assert "spa" in rota_spa.text

    assert asset.status_code == 200
    assert "console.log" in asset.text


@pytest.mark.asyncio
async def test_com_build_nao_intercepta_rotas_de_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(app_module, "FRONTEND_DIST", dist)
    api = app_module.create_app()

    transport = ASGITransport(app=api)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        resposta = await client.get("/api/health")

    assert resposta.status_code == 200
    assert resposta.json() == {"status": "ok"}
