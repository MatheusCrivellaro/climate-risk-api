"""Testes e2e dos endpoints ``/execucoes`` (UC-02 — Slice 6)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.conversores_tempo import datetime_para_iso
from climate_risk.infrastructure.db.modelos import ExecucaoORM, JobORM

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini"
FIXTURE_NC = FIXTURES / "cordex_sintetico_basico.nc"


def _corpo_basico(**overrides: Any) -> dict[str, Any]:
    corpo: dict[str, Any] = {
        "arquivo_nc": str(FIXTURE_NC),
        "cenario": "rcp45",
        "variavel": "pr",
        "parametros_indices": {
            "freq_thr_mm": 20.0,
            "p95_wet_thr": 1.0,
            "heavy20": 20.0,
            "heavy50": 50.0,
            "p95_baseline": {"inicio": 2026, "fim": 2030},
        },
    }
    corpo.update(overrides)
    return corpo


# ---------------------------------------------------------------------
# POST /execucoes
# ---------------------------------------------------------------------
@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.asyncio
async def test_criar_execucao_retorna_202_e_enfileira_job(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    resposta = await cliente_api.post("/execucoes", json=_corpo_basico())

    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    execucao_id = corpo["execucao_id"]
    job_id = corpo["job_id"]
    assert execucao_id.startswith("exec_")
    assert job_id.startswith("job_")
    assert corpo["status"] == StatusExecucao.PENDING
    assert corpo["links"] == {
        "self": f"/execucoes/{execucao_id}",
        "job": f"/jobs/{job_id}",
    }

    async with async_sessionmaker_() as sessao:
        execucao = (
            await sessao.execute(select(ExecucaoORM).where(ExecucaoORM.id == execucao_id))
        ).scalar_one()
        assert execucao.status == StatusExecucao.PENDING
        assert execucao.tipo == "grade_bbox"
        assert execucao.job_id == job_id

        job = (await sessao.execute(select(JobORM).where(JobORM.id == job_id))).scalar_one()
        assert job.status == StatusJob.PENDING
        assert job.tipo == "processar_cordex"
        payload = json.loads(job.payload)
        assert payload["execucao_id"] == execucao_id
        assert payload["p95_baseline"] == {"inicio": 2026, "fim": 2030}


@pytest.mark.asyncio
async def test_criar_execucao_arquivo_inexistente_retorna_404(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.post("/execucoes", json=_corpo_basico(arquivo_nc="/nao/existe.nc"))
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"].endswith("/arquivo-nc-nao-encontrado")
    assert corpo["status"] == 404


@pytest.mark.asyncio
async def test_criar_execucao_bbox_invalida_retorna_422(
    cliente_api: AsyncClient,
) -> None:
    # lat_min > lat_max dispara o validator do schema.
    resposta = await cliente_api.post(
        "/execucoes",
        json=_corpo_basico(
            bbox={"lat_min": 10.0, "lat_max": 5.0, "lon_min": -50.0, "lon_max": -40.0}
        ),
    )
    assert resposta.status_code == 422


# ---------------------------------------------------------------------
# GET /execucoes + /execucoes/{id}
# ---------------------------------------------------------------------
async def _inserir_execucao(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    cenario: str = "rcp45",
    status: str = StatusExecucao.PENDING,
) -> str:
    eid = gerar_id("exec")
    agora_iso = datetime_para_iso(utc_now())
    async with sessionmaker() as sessao:
        sessao.add(
            ExecucaoORM(
                id=eid,
                cenario=cenario,
                variavel="pr",
                arquivo_origem=str(FIXTURE_NC),
                tipo="grade_bbox",
                parametros=json.dumps({}),
                status=status,
                criado_em=agora_iso,
                concluido_em=None,
                job_id=None,
            )
        )
        await sessao.commit()
    return eid


@pytest.mark.asyncio
async def test_listar_execucoes_com_filtro_por_status(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _inserir_execucao(async_sessionmaker_, status=StatusExecucao.PENDING)
    await _inserir_execucao(async_sessionmaker_, status=StatusExecucao.COMPLETED)

    resposta = await cliente_api.get("/execucoes")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total"] == 2

    filtrado = (await cliente_api.get("/execucoes", params={"status": "pending"})).json()
    assert filtrado["total"] == 1
    assert filtrado["items"][0]["status"] == StatusExecucao.PENDING


@pytest.mark.asyncio
async def test_obter_execucao_por_id(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    eid = await _inserir_execucao(async_sessionmaker_)
    resposta = await cliente_api.get(f"/execucoes/{eid}")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["id"] == eid
    assert corpo["tipo"] == "grade_bbox"


@pytest.mark.asyncio
async def test_obter_execucao_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/execucoes/exec_fantasma")
    assert resposta.status_code == 404
    assert resposta.json()["type"].endswith("/entidade-nao-encontrada")


# ---------------------------------------------------------------------
# POST /execucoes/{id}/cancelar
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cancelar_execucao_pending_retorna_200(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    eid = await _inserir_execucao(async_sessionmaker_, status=StatusExecucao.PENDING)
    resposta = await cliente_api.post(f"/execucoes/{eid}/cancelar")
    assert resposta.status_code == 200, resposta.text
    assert resposta.json()["status"] == StatusExecucao.CANCELED


@pytest.mark.asyncio
async def test_cancelar_execucao_em_estado_invalido_retorna_409(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    eid = await _inserir_execucao(async_sessionmaker_, status=StatusExecucao.COMPLETED)
    resposta = await cliente_api.post(f"/execucoes/{eid}/cancelar")
    assert resposta.status_code == 409
    assert resposta.json()["type"].endswith("/job-estado-invalido")


@pytest.mark.asyncio
async def test_cancelar_execucao_inexistente_retorna_404(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.post("/execucoes/exec_fantasma/cancelar")
    assert resposta.status_code == 404
