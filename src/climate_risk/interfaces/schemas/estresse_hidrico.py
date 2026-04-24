"""Schemas Pydantic para os endpoints de estresse hídrico (Slice 15)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ParametrosIndicesEstresseHidricoSchema(BaseModel):
    """Limiares dos índices (espelha o dataclass de domínio)."""

    limiar_pr_mm_dia: float = Field(
        1.0,
        ge=0,
        description="Teto de precipitação para considerar um dia 'seco' (mm/dia).",
    )
    limiar_tas_c: float = Field(
        30.0,
        ge=-50,
        le=60,
        description="Piso de temperatura para considerar um dia 'quente' (°C).",
    )


class CriarExecucaoEstresseHidricoRequest(BaseModel):
    """Corpo do ``POST /api/execucoes/estresse-hidrico``."""

    arquivo_pr: str = Field(
        ..., min_length=1, description="Caminho local do NetCDF de precipitação."
    )
    arquivo_tas: str = Field(
        ..., min_length=1, description="Caminho local do NetCDF de temperatura."
    )
    arquivo_evap: str = Field(
        ..., min_length=1, description="Caminho local do NetCDF de evaporação."
    )
    cenario: str = Field(
        ...,
        pattern=r"^(rcp\d{2}|ssp\d{3})$",
        description="Rótulo CORDEX (ex.: 'rcp45', 'rcp85', 'ssp245').",
    )
    parametros: ParametrosIndicesEstresseHidricoSchema = Field(
        default_factory=lambda: ParametrosIndicesEstresseHidricoSchema(
            limiar_pr_mm_dia=1.0,
            limiar_tas_c=30.0,
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "arquivo_pr": "/dados/cordex/rcp45/pr_day_BR_2026-2035.nc",
                "arquivo_tas": "/dados/cordex/rcp45/tas_day_BR_2026-2035.nc",
                "arquivo_evap": "/dados/cordex/rcp45/evspsbl_day_BR_2026-2035.nc",
                "cenario": "rcp45",
                "parametros": {"limiar_pr_mm_dia": 1.0, "limiar_tas_c": 30.0},
            }
        }
    }


class CriarExecucaoEstresseHidricoResponse(BaseModel):
    """Corpo do ``202 Accepted``."""

    execucao_id: str
    job_id: str
    status: str
    criado_em: str
    links: dict[str, str] = Field(
        ...,
        description="HATEOAS: ``self`` e ``job``.",
        examples=[{"self": "/api/execucoes/exec_01HX...", "job": "/api/jobs/job_01HX..."}],
    )


class ResultadoEstresseHidricoSchema(BaseModel):
    """Item de :class:`ListarResultadosEstresseHidricoResponse`."""

    id: str
    execucao_id: str
    municipio_id: int
    ano: int
    cenario: str
    frequencia_dias_secos_quentes: int
    intensidade_mm: float
    nome_municipio: str | None = None
    uf: str | None = None


class ListarResultadosEstresseHidricoResponse(BaseModel):
    """Corpo do ``GET /api/resultados/estresse-hidrico``."""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    items: list[ResultadoEstresseHidricoSchema]
