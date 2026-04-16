"""Middleware que converte exceções não tratadas em Problem Details (RFC 7807)."""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from climate_risk.core.logging import correlation_id_ctx

logger = logging.getLogger(__name__)


class ErroRfc7807Middleware(BaseHTTPMiddleware):
    """Captura ``Exception`` genéricas e devolve resposta JSON padronizada.

    No Slice 0 apenas exceções não previstas são interceptadas. Exceções de
    domínio (``ErroArquivoNCNaoEncontrado`` e companhia) serão tratadas
    especificamente nos próximos slices.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            logger.exception("Erro não tratado processando requisição")
            correlation_id = correlation_id_ctx.get()
            payload = {
                "type": "about:blank",
                "title": "Erro interno do servidor",
                "status": 500,
                "detail": str(exc) or "Erro inesperado.",
                "instance": str(request.url.path),
                "correlation_id": correlation_id,
            }
            return JSONResponse(
                status_code=500,
                content=payload,
                media_type="application/problem+json",
            )
