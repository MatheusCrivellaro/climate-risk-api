"""Schemas Pydantic de ``POST /localizacoes/localizar`` (Slice 9)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PontoParaLocalizarRequest(BaseModel):
    """Par (lat, lon) a ser resolvido em município."""

    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    identificador: str | None = Field(
        default=None,
        description="Opcional — ecoado de volta para correlação do cliente.",
    )


class LocalizarPontosRequest(BaseModel):
    """Corpo da requisição ``POST /localizacoes/localizar``."""

    pontos: list[PontoParaLocalizarRequest] = Field(
        ...,
        min_length=1,
        max_length=5000,
        description="Lote de pontos. Operação é sempre síncrona.",
    )


class PontoLocalizadoResponse(BaseModel):
    """Resposta unitária."""

    lat: float
    lon: float
    identificador: str | None = None
    encontrado: bool
    municipio_id: int | None = None
    uf: str | None = None
    nome_municipio: str | None = None


class LocalizarPontosResponse(BaseModel):
    """Resposta agregada."""

    total: int = Field(..., ge=0)
    encontrados: int = Field(..., ge=0)
    itens: list[PontoLocalizadoResponse]
