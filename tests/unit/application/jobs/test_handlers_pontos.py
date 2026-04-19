"""Testes unitários da fábrica :func:`criar_handler_calcular_pontos`.

Verifica deserialização de ``pontos``/``parametros_indices``/baseline do
payload JSON em :class:`ParametrosProcessamentoPontos`.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from climate_risk.application.calculos.calcular_por_pontos import PontoEntradaDominio
from climate_risk.application.calculos.processar_pontos_lote import (
    ParametrosProcessamentoPontos,
    ResultadoProcessamentoPontos,
)
from climate_risk.application.jobs.handlers_pontos import criar_handler_calcular_pontos
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline


@dataclass
class _CasoUsoFake:
    recebidos: list[ParametrosProcessamentoPontos]

    async def executar(self, params: ParametrosProcessamentoPontos) -> ResultadoProcessamentoPontos:
        self.recebidos.append(params)
        return ResultadoProcessamentoPontos(
            execucao_id=params.execucao_id,
            total_pontos=len(params.pontos),
            total_resultados=0,
        )


@pytest.mark.asyncio
async def test_deserializa_payload_completo() -> None:
    caso = _CasoUsoFake(recebidos=[])
    handler = criar_handler_calcular_pontos(caso)  # type: ignore[arg-type]

    payload = {
        "execucao_id": "exec_abc",
        "arquivo_nc": "/tmp/x.nc",
        "cenario": "rcp45",
        "variavel": "pr",
        "pontos": [
            {"lat": -22.9, "lon": -46.5, "identificador": "A"},
            {"lat": -23.1, "lon": -45.9, "identificador": "B"},
        ],
        "parametros_indices": {"freq_thr_mm": 20.0, "heavy_thresholds": [20.0, 50.0]},
        "p95_baseline": {"inicio": 2026, "fim": 2030},
        "p95_wet_thr": 1.0,
    }
    await handler(payload)

    assert len(caso.recebidos) == 1
    p = caso.recebidos[0]
    assert p.execucao_id == "exec_abc"
    assert p.arquivo_nc == "/tmp/x.nc"
    assert p.cenario == "rcp45"
    assert p.variavel == "pr"
    assert p.pontos == [
        PontoEntradaDominio(lat=-22.9, lon=-46.5, identificador="A"),
        PontoEntradaDominio(lat=-23.1, lon=-45.9, identificador="B"),
    ]
    assert p.parametros_indices == ParametrosIndices(
        freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)
    )
    assert p.p95_baseline == PeriodoBaseline(2026, 2030)
    assert p.p95_wet_thr == 1.0


@pytest.mark.asyncio
async def test_baseline_e_identificador_nulos() -> None:
    caso = _CasoUsoFake(recebidos=[])
    handler = criar_handler_calcular_pontos(caso)  # type: ignore[arg-type]
    payload = {
        "execucao_id": "exec_abc",
        "arquivo_nc": "/tmp/x.nc",
        "cenario": "rcp45",
        "variavel": "pr",
        "pontos": [{"lat": -22.9, "lon": -46.5, "identificador": None}],
        "parametros_indices": {"freq_thr_mm": 20.0, "heavy_thresholds": [20.0, 50.0]},
        "p95_baseline": None,
        "p95_wet_thr": 1.0,
    }
    await handler(payload)
    p = caso.recebidos[0]
    assert p.pontos[0].identificador is None
    assert p.p95_baseline is None


@pytest.mark.asyncio
async def test_baseline_ausente_equivale_a_nulo() -> None:
    caso = _CasoUsoFake(recebidos=[])
    handler = criar_handler_calcular_pontos(caso)  # type: ignore[arg-type]
    payload = {
        "execucao_id": "exec_abc",
        "arquivo_nc": "/tmp/x.nc",
        "cenario": "rcp45",
        "variavel": "pr",
        "pontos": [{"lat": -22.9, "lon": -46.5, "identificador": "A"}],
        "parametros_indices": {"freq_thr_mm": 20.0, "heavy_thresholds": [20.0, 50.0]},
        # Sem chave "p95_baseline".
        "p95_wet_thr": 1.0,
    }
    await handler(payload)
    p = caso.recebidos[0]
    assert p.p95_baseline is None
