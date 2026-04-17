"""Schemas Pydantic para endpoints ``/jobs``."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class JobResponse(BaseModel):
    """Representação HTTP de um :class:`Job`."""

    id: str = Field(..., description="Identificador ULID prefixado (``job_...``).")
    tipo: str = Field(..., description="Categoria do job (ex.: ``noop``).")
    payload: dict[str, Any] = Field(..., description="Dados de entrada do job.")
    status: str = Field(
        ...,
        description="Um de: pending, running, completed, failed, canceled.",
    )
    tentativas: int = Field(..., ge=0)
    max_tentativas: int = Field(..., ge=1)
    criado_em: datetime
    iniciado_em: datetime | None = None
    concluido_em: datetime | None = None
    heartbeat: datetime | None = None
    erro: str | None = None
    proxima_tentativa_em: datetime | None = None


class ListaJobsResponse(BaseModel):
    """Resposta paginada de ``GET /jobs``."""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=0)
    offset: int = Field(..., ge=0)
    items: list[JobResponse] = Field(default_factory=list)
