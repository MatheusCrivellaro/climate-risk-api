"""Testes e2e dos endpoints ``/jobs`` (Slice 5)."""

from __future__ import annotations

import json

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.conversores_tempo import datetime_para_iso
from climate_risk.infrastructure.db.modelos import JobORM


async def _inserir_job(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    tipo: str = "noop",
    status: str = StatusJob.PENDING,
    erro: str | None = None,
) -> str:
    jid = gerar_id("job")
    agora_iso = datetime_para_iso(utc_now())
    async with sessionmaker() as sessao:
        sessao.add(
            JobORM(
                id=jid,
                tipo=tipo,
                payload=json.dumps({"marca": "teste"}),
                status=status,
                tentativas=0,
                max_tentativas=3,
                criado_em=agora_iso,
                iniciado_em=None,
                concluido_em=None,
                heartbeat=None,
                erro=erro,
                proxima_tentativa_em=None,
            )
        )
        await sessao.commit()
    return jid


# ---------------------------------------------------------------------
# Teste 13 — GET /jobs lista
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_listar_jobs(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _inserir_job(async_sessionmaker_, status=StatusJob.PENDING)
    await _inserir_job(async_sessionmaker_, status=StatusJob.COMPLETED)

    resposta = await cliente_api.get("/jobs")
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total"] == 2
    assert len(corpo["items"]) == 2

    # Filtro por status.
    resposta_filtrada = await cliente_api.get("/jobs", params={"status": "pending"})
    assert resposta_filtrada.status_code == 200
    filtrado = resposta_filtrada.json()
    assert filtrado["total"] == 1
    assert filtrado["items"][0]["status"] == "pending"


# ---------------------------------------------------------------------
# Teste 14 — GET /jobs/{id}
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_obter_job_por_id(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    job_id = await _inserir_job(async_sessionmaker_)

    resposta = await cliente_api.get(f"/jobs/{job_id}")
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["id"] == job_id
    assert corpo["tipo"] == "noop"
    assert corpo["payload"] == {"marca": "teste"}


@pytest.mark.asyncio
async def test_obter_job_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/jobs/job_fantasma")
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"].endswith("/job-nao-encontrado")
    assert corpo["status"] == 404
    assert "correlation_id" in corpo


# ---------------------------------------------------------------------
# Teste 15 — POST /jobs/{id}/retry
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_retry_job_failed_volta_para_pending(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    job_id = await _inserir_job(async_sessionmaker_, status=StatusJob.FAILED, erro="boom")

    resposta = await cliente_api.post(f"/jobs/{job_id}/retry")
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["status"] == "pending"
    assert corpo["tentativas"] == 0
    assert corpo["erro"] is None


@pytest.mark.asyncio
async def test_retry_job_pending_retorna_409(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    job_id = await _inserir_job(async_sessionmaker_, status=StatusJob.PENDING)

    resposta = await cliente_api.post(f"/jobs/{job_id}/retry")
    assert resposta.status_code == 409
    corpo = resposta.json()
    assert corpo["type"].endswith("/job-estado-invalido")
    assert corpo["status"] == 409


@pytest.mark.asyncio
async def test_retry_job_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post("/jobs/job_fantasma/retry")
    assert resposta.status_code == 404
    assert resposta.json()["type"].endswith("/job-nao-encontrado")
