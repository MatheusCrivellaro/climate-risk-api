"""Schemas Pydantic para os endpoints ``/resultados`` (Slice 11)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ResultadoResponse(BaseModel):
    """Representação de :class:`ResultadoIndice` na API."""

    id: str
    execucao_id: str
    lat: float
    lon: float
    lat_input: float | None = None
    lon_input: float | None = None
    ano: int
    nome_indice: str
    valor: float | None
    unidade: str | None = None
    municipio_id: int | None = None


class PaginaResultadosResponse(BaseModel):
    """Corpo do ``GET /resultados``."""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    items: list[ResultadoResponse]


class GrupoAgregadoResponse(BaseModel):
    """Uma linha do ``GET /resultados/agregados``."""

    grupo: dict[str, str | int] = Field(
        default_factory=dict,
        description="Chaves do GROUP BY com os valores daquele bucket.",
    )
    valor: float | None = Field(
        ..., description="Resultado da função; null se não definido para o grupo."
    )
    n_amostras: int = Field(..., ge=0)


class AgregacaoResponse(BaseModel):
    """Corpo do ``GET /resultados/agregados``."""

    agregacao: str
    agrupar_por: list[str] = Field(default_factory=list)
    grupos: list[GrupoAgregadoResponse]


class EstatisticasResponse(BaseModel):
    """Corpo do ``GET /resultados/stats``."""

    cenarios: list[str] = Field(default_factory=list)
    anos: list[int] = Field(default_factory=list)
    variaveis: list[str] = Field(default_factory=list)
    nomes_indices: list[str] = Field(default_factory=list)
    total_execucoes_com_resultados: int = Field(..., ge=0)
    total_resultados: int = Field(..., ge=0)
