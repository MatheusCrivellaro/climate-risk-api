"""Factory do aplicativo FastAPI."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from climate_risk.core.config import get_settings
from climate_risk.core.logging import configure_logging
from climate_risk.interfaces.middleware.correlation_id import CorrelationIdMiddleware
from climate_risk.interfaces.middleware.erros import ErroRfc7807Middleware
from climate_risk.interfaces.rotas import (
    admin,
    calculos,
    cobertura,
    execucoes,
    fornecedores,
    geocoding,
    health,
    jobs,
    resultados,
)

VERSAO_API = "0.0.1"

# Caminho esperado para o build do frontend (`frontend/dist/`).
# `app.py` está em `src/climate_risk/interfaces/`, então subir 3 níveis
# leva à raiz do repositório.
FRONTEND_DIST = Path(__file__).resolve().parents[3] / "frontend" / "dist"

FRONTEND_NAO_BUILDADO_HTML = (
    "<h1>Frontend não disponível</h1>"
    "<p>Build do frontend não encontrado em <code>frontend/dist/</code>.</p>"
    "<p>Rode <code>cd frontend && pnpm install && pnpm build</code> e reinicie o servidor.</p>"
    "<p>Durante desenvolvimento, prefira <code>pnpm dev</code> em "
    "<a href='http://localhost:5173'>localhost:5173</a>.</p>"
)


def create_app() -> FastAPI:
    """Cria e retorna a instância FastAPI configurada."""
    settings = get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(
        title="Climate Risk API",
        version=VERSAO_API,
        description=(
            "API para cálculo de índices de frequência e intensidade de "
            "precipitação sobre dados CORDEX."
        ),
    )

    # A ordem importa: quem é adicionado por último executa primeiro.
    app.add_middleware(ErroRfc7807Middleware)
    app.add_middleware(CorrelationIdMiddleware)

    api = APIRouter(prefix="/api")
    api.include_router(health.router)
    api.include_router(calculos.router)
    api.include_router(execucoes.router)
    api.include_router(jobs.router)
    api.include_router(geocoding.router)
    api.include_router(cobertura.router)
    api.include_router(fornecedores.router)
    api.include_router(resultados.router)
    api.include_router(admin.router)
    app.include_router(api)

    _montar_frontend(app)

    return app


def _montar_frontend(app: FastAPI) -> None:
    """Monta o build do frontend em ``/app/`` quando disponível.

    Em modo dev o frontend roda via ``pnpm dev`` (Vite) em outra porta
    e o build não existe — servimos uma mensagem 503 explicativa.
    """
    if FRONTEND_DIST.exists():
        app.mount(
            "/app/assets",
            StaticFiles(directory=FRONTEND_DIST / "assets"),
            name="frontend-assets",
        )
        index_html = FRONTEND_DIST / "index.html"

        @app.get("/app", include_in_schema=False)
        @app.get("/app/{full_path:path}", include_in_schema=False)
        async def servir_frontend(full_path: str = "") -> FileResponse:
            return FileResponse(index_html)
    else:

        @app.get("/app", include_in_schema=False)
        @app.get("/app/{full_path:path}", include_in_schema=False)
        async def frontend_nao_buildado(full_path: str = "") -> HTMLResponse:
            return HTMLResponse(status_code=503, content=FRONTEND_NAO_BUILDADO_HTML)


app = create_app()
