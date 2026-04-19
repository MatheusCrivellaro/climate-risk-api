"""Schemas Pydantic de ``/fornecedores`` (Slice 10)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FornecedorRequest(BaseModel):
    """Corpo de ``POST /fornecedores``."""

    nome: str = Field(..., min_length=1, max_length=200)
    cidade: str = Field(..., min_length=1, max_length=120)
    uf: str = Field(..., min_length=2, max_length=2)
    identificador_externo: str | None = Field(default=None, max_length=120)
    municipio_id: int | None = None


class FornecedorResponse(BaseModel):
    """Representação de um :class:`Fornecedor` em JSON."""

    id: str
    nome: str
    cidade: str
    uf: str
    identificador_externo: str | None = None
    lat: float | None = None
    lon: float | None = None
    municipio_id: int | None = None
    criado_em: str
    atualizado_em: str


class PaginaFornecedoresResponse(BaseModel):
    """Resposta paginada de ``GET /fornecedores``."""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    itens: list[FornecedorResponse]


class ErroLinhaResponse(BaseModel):
    linha: int = Field(..., ge=1)
    motivo: str


class ResultadoImportacaoResponse(BaseModel):
    """Resposta de ``POST /fornecedores/importar``."""

    total_linhas: int = Field(..., ge=0)
    importados: int = Field(..., ge=0)
    duplicados: int = Field(..., ge=0)
    erros: list[ErroLinhaResponse]
