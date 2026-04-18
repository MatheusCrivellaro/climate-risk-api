"""Schemas Pydantic para o endpoint ``POST /calculos/pontos`` (UC-03 síncrono)."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class PontoEntrada(BaseModel):
    """Coordenada de entrada para um ponto (fornecedor, sítio, etc.)."""

    lat: float = Field(..., ge=-90.0, le=90.0, description="Latitude em graus decimais.")
    lon: float = Field(..., ge=-180.0, le=180.0, description="Longitude em graus decimais.")
    identificador: str | None = Field(
        default=None,
        max_length=64,
        description="Rótulo opcional (ex.: ``forn-001``) devolvido em cada resultado.",
    )


class PeriodoBaselineEntrada(BaseModel):
    """Intervalo fechado de anos usado como baseline para o P95."""

    inicio: int = Field(..., ge=1850, le=2300, description="Ano inicial (inclusivo).")
    fim: int = Field(..., ge=1850, le=2300, description="Ano final (inclusivo).")

    @model_validator(mode="after")
    def _validar_ordem(self) -> PeriodoBaselineEntrada:
        if self.inicio > self.fim:
            raise ValueError("p95_baseline.inicio deve ser <= p95_baseline.fim.")
        return self


class ParametrosIndicesEntrada(BaseModel):
    """Parâmetros configuráveis do cálculo."""

    freq_thr_mm: float = Field(
        default=20.0,
        ge=0.0,
        description="Limiar T (mm/dia) para wet_days/sdii.",
    )
    p95_wet_thr: float = Field(
        default=1.0,
        ge=0.0,
        description="Limiar de dia chuvoso (mm/dia) para o cálculo do P95.",
    )
    heavy20: float = Field(default=20.0, ge=0.0, description="Limiar para r20mm (mm/dia).")
    heavy50: float = Field(default=50.0, ge=0.0, description="Limiar para r50mm (mm/dia).")
    p95_baseline: PeriodoBaselineEntrada | None = Field(
        default=None,
        description="Baseline do P95; ``null`` desativa o cálculo do P95.",
    )

    @model_validator(mode="after")
    def _validar_heavy(self) -> ParametrosIndicesEntrada:
        if self.heavy20 > self.heavy50:
            raise ValueError("heavy20 deve ser <= heavy50.")
        return self


class CalculoPorPontosRequest(BaseModel):
    """Corpo do ``POST /calculos/pontos``."""

    arquivo_nc: str = Field(..., min_length=1, description="Caminho absoluto do arquivo NetCDF.")
    cenario: str = Field(..., min_length=1, max_length=32, description="Rótulo do cenário.")
    variavel: str = Field(
        default="pr",
        min_length=1,
        max_length=32,
        description="Nome da variável climática (MVP: ``pr``).",
    )
    pontos: list[PontoEntrada] = Field(
        ..., min_length=1, description="Pontos a avaliar. Limite síncrono validado na rota."
    )
    parametros_indices: ParametrosIndicesEntrada = Field(
        default_factory=ParametrosIndicesEntrada,
        description="Parâmetros dos índices e do P95.",
    )
    persistir: bool = Field(
        default=False,
        description="Se ``true``, grava Execucao + ResultadoIndice no banco.",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "arquivo_nc": "/dados/cordex/rcp45/pr_day_BR_2026-2030.nc",
                "cenario": "rcp45",
                "variavel": "pr",
                "pontos": [{"lat": -23.55, "lon": -46.63, "identificador": "forn-001"}],
                "parametros_indices": {
                    "freq_thr_mm": 20.0,
                    "p95_wet_thr": 1.0,
                    "heavy20": 20.0,
                    "heavy50": 50.0,
                    "p95_baseline": {"inicio": 2026, "fim": 2035},
                },
                "persistir": True,
            }
        }
    }


class IndicesResposta(BaseModel):
    """Conjunto de índices anuais retornado por ponto/ano."""

    wet_days: int
    sdii: float | None
    rx1day: float | None
    rx5day: float | None
    r20mm: int
    r50mm: int
    r95ptot_mm: float | None
    r95ptot_frac: float | None


class PontoResultado(BaseModel):
    """Uma linha de resultado — um ponto em um ano."""

    identificador: str | None
    lat_input: float
    lon_input: float
    lat_grid: float
    lon_grid: float
    ano: int
    indices: IndicesResposta


class CalculoPorPontosResponse(BaseModel):
    """Corpo do ``200 OK`` do endpoint síncrono."""

    execucao_id: str | None = Field(
        default=None,
        description="ID da Execucao persistida; ``null`` quando ``persistir=false``.",
    )
    cenario: str
    variavel: str
    total_pontos: int = Field(..., ge=0, description="Quantidade de pontos recebidos.")
    total_resultados: int = Field(
        ..., ge=0, description="Quantidade de linhas ``(ponto, ano)`` retornadas."
    )
    resultados: list[PontoResultado]


class CalculoPontosAsyncResponse(BaseModel):
    """Corpo do ``202 Accepted`` quando o lote excede o limite síncrono.

    Quando ``len(pontos) > settings.sincrono_pontos_max``, a rota enfileira
    um :class:`Job` do tipo ``calcular_pontos`` e devolve esta resposta.
    O cliente deve acompanhar o progresso via ``GET /execucoes/{id}`` ou
    ``GET /jobs/{id}``.
    """

    execucao_id: str = Field(..., description="ID da Execucao criada em ``pending``.")
    job_id: str = Field(..., description="ID do Job enfileirado para processamento.")
    status: str = Field(..., description="Status inicial da execução (``pending``).")
    total_pontos: int = Field(..., ge=0, description="Total de pontos no lote.")
    criado_em: str = Field(..., description="Timestamp ISO 8601 (UTC) da criação.")
    links: dict[str, str] = Field(
        ...,
        description="Hyperlinks navegáveis (``self`` para execução, ``job`` para o job).",
    )
