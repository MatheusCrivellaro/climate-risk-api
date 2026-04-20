"""Schemas dos endpoints ``/admin`` (Slice 12).

Agrupa respostas administrativas que não pertencem a um domínio específico:

- :class:`AdminStatsResponse`: sumário operacional (``GET /admin/stats``).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ContadoresAdmin(BaseModel):
    """Contadores simples de entidades persistidas."""

    fornecedores: int = Field(..., ge=0)
    municipios: int = Field(..., ge=0)
    jobs: int = Field(..., ge=0)
    execucoes: int = Field(..., ge=0)


class AdminStatsResponse(BaseModel):
    """Corpo do ``GET /admin/stats``.

    Reúne os contadores operacionais e as estatísticas já oferecidas por
    :class:`~climate_risk.application.resultados.ConsultarStats` — evita que
    painéis tenham de bater em dois endpoints para montar um overview.
    """

    contadores: ContadoresAdmin
    cenarios: list[str] = Field(default_factory=list)
    anos: list[int] = Field(default_factory=list)
    variaveis: list[str] = Field(default_factory=list)
    nomes_indices: list[str] = Field(default_factory=list)
    total_execucoes_com_resultados: int = Field(..., ge=0)
    total_resultados: int = Field(..., ge=0)
