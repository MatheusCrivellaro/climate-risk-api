"""Testes e2e do roteamento síncrono/assíncrono de ``POST /calculos/pontos`` (Slice 7).

Cobre:

- Lote pequeno (``<= sincrono_pontos_max``) → 200 síncrono.
- Lote grande (``> sincrono_pontos_max``) → 202 com ``execucao_id``/``job_id``
  e persistência da :class:`Execucao` em ``pending`` + :class:`Job` em
  ``pending`` com payload correto.
- Arquivo ausente no ramo assíncrono → 404 ``ProblemDetails``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.config import get_settings
from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.modelos import ExecucaoORM, JobORM

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini"
FIXTURE_NC = FIXTURES / "cordex_sintetico_basico.nc"


def _pontos(n: int) -> list[dict[str, object]]:
    return [
        {"lat": -22.9 + 0.001 * i, "lon": -46.5, "identificador": f"P{i:04d}"} for i in range(n)
    ]


def _corpo(pontos: list[dict[str, object]], *, arquivo_nc: str | None = None) -> dict[str, object]:
    return {
        "arquivo_nc": arquivo_nc if arquivo_nc is not None else str(FIXTURE_NC),
        "cenario": "rcp45",
        "variavel": "pr",
        "pontos": pontos,
        "parametros_indices": {
            "freq_thr_mm": 20.0,
            "p95_wet_thr": 1.0,
            "heavy20": 20.0,
            "heavy50": 50.0,
            "p95_baseline": {"inicio": 2026, "fim": 2030},
        },
    }


@pytest.mark.skipif(not FIXTURE_NC.exists(), reason="Fixture sintética básica ausente.")
@pytest.mark.asyncio
async def test_limite_exato_ainda_retorna_200_sincrono(cliente_api: AsyncClient) -> None:
    limite = get_settings().sincrono_pontos_max
    resposta = await cliente_api.post("/api/calculos/pontos", json=_corpo(_pontos(limite)))
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total_pontos"] == limite
    assert corpo["total_resultados"] >= limite


@pytest.mark.skipif(not FIXTURE_NC.exists(), reason="Fixture sintética básica ausente.")
@pytest.mark.asyncio
async def test_excede_limite_retorna_202_com_execucao_e_job(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    limite = get_settings().sincrono_pontos_max
    n = limite + 1
    resposta = await cliente_api.post("/api/calculos/pontos", json=_corpo(_pontos(n)))

    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    assert corpo["status"] == StatusExecucao.PENDING
    assert corpo["total_pontos"] == n
    execucao_id = corpo["execucao_id"]
    job_id = corpo["job_id"]
    assert isinstance(execucao_id, str) and execucao_id.startswith("exec_")
    assert isinstance(job_id, str) and job_id.startswith("job_")
    assert corpo["links"]["self"] == f"/api/execucoes/{execucao_id}"
    assert corpo["links"]["job"] == f"/api/jobs/{job_id}"

    async with async_sessionmaker_() as sessao:
        execucao = (
            await sessao.execute(select(ExecucaoORM).where(ExecucaoORM.id == execucao_id))
        ).scalar_one()
        assert execucao.tipo == "pontos_lote"
        assert execucao.status == StatusExecucao.PENDING
        assert execucao.job_id == job_id
        assert execucao.concluido_em is None

        job = (await sessao.execute(select(JobORM).where(JobORM.id == job_id))).scalar_one()
        assert job.tipo == "calcular_pontos"
        assert job.status == StatusJob.PENDING
        payload = json.loads(job.payload)
        assert payload["execucao_id"] == execucao_id
        assert payload["arquivo_nc"] == str(FIXTURE_NC)
        assert payload["cenario"] == "rcp45"
        assert payload["variavel"] == "pr"
        assert payload["p95_baseline"] == {"inicio": 2026, "fim": 2030}
        assert len(payload["pontos"]) == n
        assert payload["pontos"][0]["identificador"] == "P0000"


@pytest.mark.asyncio
async def test_arquivo_inexistente_ramo_async_retorna_404(cliente_api: AsyncClient) -> None:
    limite = get_settings().sincrono_pontos_max
    resposta = await cliente_api.post(
        "/api/calculos/pontos",
        json=_corpo(_pontos(limite + 1), arquivo_nc="/nao/existe.nc"),
    )
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"].endswith("/arquivo-nc-nao-encontrado")
    assert corpo["status"] == 404
    assert "correlation_id" in corpo
