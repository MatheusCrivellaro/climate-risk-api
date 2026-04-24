"""Middleware que converte exceções em Problem Details (RFC 7807)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from climate_risk.core.logging import correlation_id_ctx
from climate_risk.domain.excecoes import (
    ErroArquivoNCNaoEncontrado,
    ErroCenarioInconsistente,
    ErroClienteIBGE,
    ErroConfiguracao,
    ErroConflito,
    ErroCoordenadasLatLonAusentes,
    ErroDimensaoTempoAusente,
    ErroDominio,
    ErroEntidadeNaoEncontrada,
    ErroFormatoInvalido,
    ErroJobEstadoInvalido,
    ErroJobNaoEncontrado,
    ErroLeituraNetCDF,
    ErroLimitePontosSincrono,
    ErroPastaVazia,
    ErroValidacao,
    ErroVariavelAusente,
)

logger = logging.getLogger(__name__)

BASE_TYPE_URI = "https://api.local/errors"


@dataclass(frozen=True)
class _MapaErro:
    status: int
    titulo: str
    slug: str


# Ordem importa: subclasses (mais específicas) devem vir antes das bases.
_MAPEAMENTO: tuple[tuple[type[Exception], _MapaErro], ...] = (
    (
        ErroLimitePontosSincrono,
        _MapaErro(400, "Limite de pontos síncronos excedido", "limite-pontos-sincrono"),
    ),
    (
        ErroFormatoInvalido,
        _MapaErro(400, "Formato de arquivo não suportado", "formato-invalido"),
    ),
    (
        ErroValidacao,
        _MapaErro(422, "Parâmetros inválidos", "validacao"),
    ),
    (
        ErroArquivoNCNaoEncontrado,
        _MapaErro(404, "Arquivo NetCDF não encontrado", "arquivo-nc-nao-encontrado"),
    ),
    (
        ErroPastaVazia,
        _MapaErro(422, "Pasta sem arquivos NetCDF", "pasta-vazia"),
    ),
    (
        ErroCenarioInconsistente,
        _MapaErro(422, "Arquivos com cenário divergente", "cenario-inconsistente"),
    ),
    (
        ErroVariavelAusente,
        _MapaErro(422, "Variável ausente no dataset", "variavel-ausente"),
    ),
    (
        ErroDimensaoTempoAusente,
        _MapaErro(422, "Dimensão 'time' ausente na variável", "dimensao-tempo-ausente"),
    ),
    (
        ErroCoordenadasLatLonAusentes,
        _MapaErro(422, "Coordenadas lat/lon não identificadas", "coords-latlon-ausentes"),
    ),
    (
        ErroLeituraNetCDF,
        _MapaErro(500, "Falha ao ler arquivo NetCDF", "leitura-netcdf"),
    ),
    (
        ErroJobNaoEncontrado,
        _MapaErro(404, "Job não encontrado", "job-nao-encontrado"),
    ),
    (
        ErroJobEstadoInvalido,
        _MapaErro(409, "Estado do job não permite a transição", "job-estado-invalido"),
    ),
    (
        ErroEntidadeNaoEncontrada,
        _MapaErro(404, "Entidade não encontrada", "entidade-nao-encontrada"),
    ),
    (
        ErroClienteIBGE,
        _MapaErro(503, "API do IBGE indisponível", "ibge-indisponivel"),
    ),
    (
        ErroConflito,
        _MapaErro(409, "Conflito de integridade", "conflito"),
    ),
    (
        ErroConfiguracao,
        _MapaErro(500, "Configuração ausente ou inválida", "configuracao"),
    ),
    (
        ErroDominio,
        _MapaErro(500, "Erro de domínio", "dominio"),
    ),
)


class ErroRfc7807Middleware(BaseHTTPMiddleware):
    """Captura exceções e devolve resposta ``application/problem+json``.

    Regras:

    - :class:`ErroDominio` e subclasses são mapeadas pelo dicionário acima.
    - Qualquer outra :class:`Exception` vira ``500`` genérico com
      ``title="Erro interno do servidor"``.
    - ``correlation_id`` é sempre incluído (vindo do :class:`ContextVar`
      populado pelo :class:`CorrelationIdMiddleware`).
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            return _para_problem_response(exc, request)


def _para_problem_response(exc: Exception, request: Request) -> JSONResponse:
    correlation_id = correlation_id_ctx.get()
    instance = str(request.url.path)

    mapa = _resolver_mapa(exc)
    if mapa is not None:
        logger.warning(
            "Exceção de domínio convertida para Problem Details: %s: %s",
            type(exc).__name__,
            exc,
        )
        payload = {
            "type": f"{BASE_TYPE_URI}/{mapa.slug}",
            "title": mapa.titulo,
            "status": mapa.status,
            "detail": str(exc),
            "instance": instance,
            "correlation_id": correlation_id,
        }
        return JSONResponse(
            status_code=mapa.status,
            content=payload,
            media_type="application/problem+json",
        )

    logger.exception("Erro não tratado processando requisição")
    payload = {
        "type": "about:blank",
        "title": "Erro interno do servidor",
        "status": 500,
        "detail": str(exc) or "Erro inesperado.",
        "instance": instance,
        "correlation_id": correlation_id,
    }
    return JSONResponse(
        status_code=500,
        content=payload,
        media_type="application/problem+json",
    )


def _resolver_mapa(exc: Exception) -> _MapaErro | None:
    for tipo, mapa in _MAPEAMENTO:
        if isinstance(exc, tipo):
            return mapa
    return None
