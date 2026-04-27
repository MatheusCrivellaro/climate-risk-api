"""Factory do aplicativo FastAPI."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI
from fastapi.staticfiles import StaticFiles

from climate_risk.core.config import get_settings
from climate_risk.core.logging import configure_logging
from climate_risk.interfaces.middleware.correlation_id import CorrelationIdMiddleware
from climate_risk.interfaces.middleware.erros import ErroRfc7807Middleware
from climate_risk.interfaces.rotas import (
    admin,
    calculos,
    cobertura,
    estresse_hidrico,
    execucoes,
    fornecedores,
    fs,
    geocoding,
    health,
    jobs,
    resultados,
)

VERSAO_API = "0.0.1"

# Página "estudo" — interface HTML/CSS/JS puro focada no pipeline de
# estresse hídrico. A partir da Slice 20 é a única interface visível.
ESTUDO_DIR = Path(__file__).resolve().parents[3] / "estudo"


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
    api.include_router(estresse_hidrico.router_execucoes)
    api.include_router(jobs.router)
    api.include_router(geocoding.router)
    api.include_router(cobertura.router)
    api.include_router(fornecedores.router)
    api.include_router(resultados.router)
    api.include_router(estresse_hidrico.router_resultados)
    api.include_router(fs.router)
    api.include_router(admin.router)
    app.include_router(api)

    _montar_estudo(app)

    # ========================================================================
    # Frontend React desativado a partir da Slice 20.
    # A interface principal agora é /estudo/ (HTML/CSS/JS puro).
    # Para reativar o /app/, descomentar as linhas abaixo:
    # ------------------------------------------------------------------------
    # _frontend_dist = Path(__file__).resolve().parents[3] / "frontend" / "dist"
    # if _frontend_dist.exists():
    #     app.mount("/app", StaticFiles(directory=_frontend_dist, html=True), name="app")
    # ========================================================================

    return app


def _montar_estudo(app: FastAPI) -> None:
    """Monta a página ``/estudo/`` (HTML/CSS/JS puro) quando o diretório existe.

    ``html=True`` faz o StaticFiles servir ``index.html`` automaticamente
    ao acessar ``/estudo/`` (com ou sem barra).
    """
    if ESTUDO_DIR.exists():
        app.mount(
            "/estudo",
            StaticFiles(directory=ESTUDO_DIR, html=True),
            name="estudo",
        )


app = create_app()
