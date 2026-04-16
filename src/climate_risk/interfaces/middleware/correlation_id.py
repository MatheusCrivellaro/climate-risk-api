"""Middleware que propaga o cabeçalho ``X-Correlation-ID`` por toda a requisição."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from ulid import ULID

from climate_risk.core.logging import correlation_id_ctx

CABECALHO_CORRELATION_ID = "X-Correlation-ID"


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Lê ou gera um ``correlation_id`` e o espelha no header de resposta."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        correlation_id = request.headers.get(CABECALHO_CORRELATION_ID)
        if not correlation_id:
            correlation_id = f"req_{ULID()}"

        token = correlation_id_ctx.set(correlation_id)
        try:
            request.state.correlation_id = correlation_id
            response = await call_next(request)
        finally:
            correlation_id_ctx.reset(token)

        response.headers[CABECALHO_CORRELATION_ID] = correlation_id
        return response
