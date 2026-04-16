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
