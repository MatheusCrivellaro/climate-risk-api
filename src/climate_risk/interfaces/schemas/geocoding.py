"""Schemas de ``POST /localizacoes/geocodificar`` e ``POST /admin/ibge/refresh``.

Todos os campos são typados em Pydantic v2 — a conversão
``dataclass → BaseModel`` acontece nos handlers de rota, nunca na camada
de aplicação.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntradaLocalizacaoSchema(BaseModel):
    """Par (cidade, uf) digitado pelo usuário."""

    cidade: str = Field(..., min_length=1, max_length=120, description="Nome da cidade.")
    uf: str = Field(..., min_length=2, max_length=2, description="Sigla da UF (2 letras).")


class LocalizacaoGeocodificadaSchema(BaseModel):
    """Resposta unitária da geocodificação."""

    cidade_entrada: str
    uf: str
    municipio_id: int | None = Field(
        default=None,
        description="Código IBGE (7 dígitos) do município resolvido, se houver.",
    )
    nome_canonico: str | None = Field(
        default=None,
        description="Nome oficial do município segundo o IBGE.",
    )
    lat: float | None = Field(
        default=None,
        description="Latitude do centroide em graus decimais (EPSG:4326).",
    )
    lon: float | None = Field(
        default=None,
        description="Longitude do centroide em graus decimais (EPSG:4326).",
    )
    metodo: str = Field(
        ...,
        description=(
            "Caminho que resolveu a entrada: 'cache_exato', 'cache_fuzzy', "
            "'ibge', 'nao_encontrado' ou 'api_falhou'."
        ),
    )


class GeocodificarRequest(BaseModel):
    """Corpo da requisição ``POST /localizacoes/geocodificar``."""

    localizacoes: list[EntradaLocalizacaoSchema] = Field(
        ..., min_length=1, description="Lote de entradas a geocodificar."
    )


class GeocodificarResponse(BaseModel):
    """Resposta agregada de ``POST /localizacoes/geocodificar``."""

    total: int = Field(..., ge=0)
    encontrados: int = Field(..., ge=0)
    nao_encontrados: int = Field(..., ge=0)
    itens: list[LocalizacaoGeocodificadaSchema]


class RefreshIBGEResponse(BaseModel):
    """Resposta de ``POST /admin/ibge/refresh``."""

    total_municipios: int = Field(..., ge=0)
    com_centroide: int = Field(..., ge=0)
    sem_centroide: int = Field(..., ge=0)
