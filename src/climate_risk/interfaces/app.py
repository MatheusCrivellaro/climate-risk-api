"""Factory do aplicativo FastAPI."""

from __future__ import annotations

from fastapi import APIRouter, FastAPI

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

    return app


app = create_app()
