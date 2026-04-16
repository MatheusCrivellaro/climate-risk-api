"""Factory do aplicativo FastAPI."""

from __future__ import annotations

from fastapi import FastAPI

from climate_risk.core.config import get_settings
from climate_risk.core.logging import configure_logging
from climate_risk.interfaces.middleware.correlation_id import CorrelationIdMiddleware
from climate_risk.interfaces.middleware.erros import ErroRfc7807Middleware
from climate_risk.interfaces.rotas import health

VERSAO_API = "0.0.1"


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

    app.include_router(health.router)

    return app


app = create_app()
