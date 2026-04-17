"""Schemas Pydantic para os endpoints ``/execucoes`` (UC-02 assíncrono)."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from climate_risk.interfaces.schemas.calculos import (
    ParametrosIndicesEntrada,
    PeriodoBaselineEntrada,
)


class BBoxEntrada(BaseModel):
    """Caixa delimitadora espacial para recorte do grid.

    Permite que o retângulo cruze o antimeridiano (``lon_min > lon_max``);
    nesse caso, a inclusão em longitude é tratada como duas faixas
    disjuntas. Validamos apenas a ordem das latitudes.
    """

    lat_min: float = Field(..., ge=-90.0, le=90.0, description="Latitude mínima (inclusiva).")
    lat_max: float = Field(..., ge=-90.0, le=90.0, description="Latitude máxima (inclusiva).")
    lon_min: float = Field(..., ge=-180.0, le=180.0, description="Longitude mínima (inclusiva).")
    lon_max: float = Field(..., ge=-180.0, le=180.0, description="Longitude máxima (inclusiva).")

    @model_validator(mode="after")
    def _validar(self) -> BBoxEntrada:
        if self.lat_min > self.lat_max:
            raise ValueError("bbox.lat_min deve ser <= lat_max.")
        return self


class CriarExecucaoRequest(BaseModel):
    """Corpo do ``POST /execucoes``."""

    arquivo_nc: str = Field(..., min_length=1, description="Caminho local para o arquivo .nc.")
    cenario: str = Field(..., min_length=1, max_length=40, description="Rótulo do cenário.")
    variavel: str = Field(
        default="pr",
        min_length=1,
        max_length=32,
        description="Nome da variável climática. MVP: apenas 'pr'.",
    )
    bbox: BBoxEntrada | None = Field(
        default=None, description="Recorte espacial opcional. None processa a grade inteira."
    )
    parametros_indices: ParametrosIndicesEntrada = Field(
        default_factory=ParametrosIndicesEntrada,
        description="Parâmetros dos índices e do P95.",
    )
    p95_baseline: PeriodoBaselineEntrada | None = Field(
        default=None,
        description=(
            "Baseline do P95. Quando ``null``, prevalece "
            "``parametros_indices.p95_baseline``; ambos ``null`` desativam o P95."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "arquivo_nc": "/dados/cordex/rcp45/pr_day_BR_2026-2030.nc",
                "cenario": "rcp45",
                "variavel": "pr",
                "bbox": {
                    "lat_min": -33.75,
                    "lat_max": 5.5,
                    "lon_min": -74.0,
                    "lon_max": -34.8,
                },
                "parametros_indices": {
                    "freq_thr_mm": 20.0,
                    "p95_wet_thr": 1.0,
                    "heavy20": 20.0,
                    "heavy50": 50.0,
                    "p95_baseline": {"inicio": 2026, "fim": 2035},
                },
            }
        }
    }


class ExecucaoResumo(BaseModel):
    """Representação resumida de :class:`Execucao` na API."""

    id: str
    cenario: str
    variavel: str
    arquivo_origem: str
    tipo: str
    status: str
    criado_em: str
    concluido_em: str | None
    job_id: str | None


class CriarExecucaoResponse(BaseModel):
    """Corpo do ``202 Accepted`` de ``POST /execucoes``."""

    execucao_id: str
    job_id: str
    status: str
    criado_em: str
    links: dict[str, str] = Field(
        ...,
        description="HATEOAS: ``self`` e ``job``.",
        examples=[{"self": "/execucoes/exec_01HX...", "job": "/jobs/job_01HX..."}],
    )


class ListaExecucoesResponse(BaseModel):
    """Corpo do ``GET /execucoes``."""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    items: list[ExecucaoResumo]
