"""Testes unitários da fábrica :func:`criar_handler_processar_cordex`.

Verifica a deserialização do payload: bbox/baseline opcionais, tipos
primitivos encaminhados a :class:`ParametrosProcessamento`.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from climate_risk.application.execucoes.processar_cenario import (
    ParametrosProcessamento,
    ResultadoProcessamento,
)
from climate_risk.application.jobs.handlers_cordex import criar_handler_processar_cordex
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline


@dataclass
class _CasoUsoFake:
    recebidos: list[ParametrosProcessamento]

    async def executar(self, params: ParametrosProcessamento) -> ResultadoProcessamento:
        self.recebidos.append(params)
        return ResultadoProcessamento(
            execucao_id=params.execucao_id,
            total_celulas=0,
            total_anos=0,
            total_resultados=0,
        )


@pytest.mark.asyncio
async def test_deserializa_payload_completo() -> None:
    caso = _CasoUsoFake(recebidos=[])
    handler = criar_handler_processar_cordex(caso)  # type: ignore[arg-type]

    payload = {
        "execucao_id": "exec_abc",
        "arquivo_nc": "/tmp/x.nc",
        "cenario": "rcp45",
        "variavel": "pr",
        "bbox": {"lat_min": -10.0, "lat_max": 0.0, "lon_min": -50.0, "lon_max": -40.0},
        "parametros_indices": {"freq_thr_mm": 20.0, "heavy_thresholds": [20.0, 50.0]},
        "p95_baseline": {"inicio": 2026, "fim": 2030},
        "p95_wet_thr": 1.0,
    }
    await handler(payload)

    assert len(caso.recebidos) == 1
    p = caso.recebidos[0]
    assert p.execucao_id == "exec_abc"
    assert p.arquivo_nc == "/tmp/x.nc"
    assert p.variavel == "pr"
    assert p.bbox == BoundingBox(-10.0, 0.0, -50.0, -40.0)
    assert p.parametros_indices == ParametrosIndices(
        freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)
    )
    assert p.p95_baseline == PeriodoBaseline(2026, 2030)
    assert p.p95_wet_thr == 1.0


@pytest.mark.asyncio
async def test_bbox_e_baseline_nulos() -> None:
    caso = _CasoUsoFake(recebidos=[])
    handler = criar_handler_processar_cordex(caso)  # type: ignore[arg-type]
    payload = {
        "execucao_id": "exec_abc",
        "arquivo_nc": "/tmp/x.nc",
        "cenario": "rcp45",
        "variavel": "pr",
        "bbox": None,
        "parametros_indices": {"freq_thr_mm": 20.0, "heavy_thresholds": [20.0, 50.0]},
        "p95_baseline": None,
        "p95_wet_thr": 1.0,
    }
    await handler(payload)
    p = caso.recebidos[0]
    assert p.bbox is None
    assert p.p95_baseline is None
