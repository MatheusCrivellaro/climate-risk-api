"""Schemas Pydantic compartilhados entre endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Pagina[T](BaseModel):
    """Resposta paginada genérica."""

    total: int = Field(..., ge=0, description="Total de itens existentes.")
    limit: int = Field(..., ge=0, description="Tamanho máximo da página.")
    offset: int = Field(..., ge=0, description="Deslocamento desde o início.")
    items: list[T] = Field(default_factory=list, description="Itens da página atual.")


class ErroRfc7807(BaseModel):
    """Representação de erro conforme RFC 7807 (Problem Details)."""

    type: str = Field(default="about:blank")
    title: str
    status: int
    detail: str | None = None
    instance: str | None = None
    correlation_id: str | None = None


class ProblemDetails(BaseModel):
    """Alias público e mais verboso de :class:`ErroRfc7807`.

    Usado nas declarações ``responses={...}`` dos endpoints para que o
    OpenAPI exponha o formato RFC 7807 nas respostas de erro (``4xx``/
    ``5xx``). Mantemos ambos os nomes: ``ErroRfc7807`` é o identificador
    interno e ``ProblemDetails`` é a exposição pública no schema.
    """

    type: str = Field(
        default="about:blank",
        description="URI que identifica o tipo de problema.",
    )
    title: str = Field(..., description="Resumo legível do problema.")
    status: int = Field(..., description="Código HTTP associado.")
    detail: str | None = Field(
        default=None, description="Explicação específica para esta ocorrência."
    )
    instance: str | None = Field(
        default=None, description="URI da ocorrência (normalmente ``request.url.path``)."
    )
    correlation_id: str | None = Field(
        default=None, description="Identificador propagado no header ``X-Correlation-ID``."
    )
