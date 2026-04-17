"""Testes de :class:`SQLAlchemyRepositorioJobs`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.infrastructure.db.repositorios import SQLAlchemyRepositorioJobs


def _fazer_job(
    *,
    id_: str | None = None,
    tipo: str = "processar_cordex",
    status: str = StatusJob.PENDING,
    tentativas: int = 0,
    payload: dict[str, object] | None = None,
) -> Job:
    return Job(
        id=id_ or gerar_id("job"),
        tipo=tipo,
        payload=payload
        or {
            "execucao_id": "exec_abc",
            "arquivo_nc": "/dados/pr.nc",
            "metadados": {"prioridade": "normal", "retries": [1, 2, 3]},
        },
        status=status,
        tentativas=tentativas,
        max_tentativas=3,
        criado_em=datetime(2026, 4, 16, 10, 30, tzinfo=UTC),
        iniciado_em=None,
        concluido_em=None,
        heartbeat=None,
        erro=None,
        proxima_tentativa_em=None,
    )


@pytest.mark.asyncio
async def test_criar_buscar_por_id(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioJobs(async_session)
    job = _fazer_job()
    await repo.salvar(job)

    lido = await repo.buscar_por_id(job.id)
    assert lido is not None
    assert lido.id == job.id
    assert lido.tipo == "processar_cordex"


@pytest.mark.asyncio
async def test_payload_complexo_preservado(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioJobs(async_session)
    payload = {
        "pontos": [
            {"lat": -23.5, "lon": -46.6, "id": "forn_001"},
            {"lat": -22.9, "lon": -43.2, "id": "forn_002"},
        ],
        "parametros": {"freq_thr_mm": 20.0, "p95_wet_thr": 1.0},
        "geocodificar": True,
    }
    job = _fazer_job(payload=payload)
    await repo.salvar(job)

    lido = await repo.buscar_por_id(job.id)
    assert lido is not None
    assert lido.payload == payload


@pytest.mark.asyncio
async def test_atualizar_status_e_tentativas(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioJobs(async_session)
    job = _fazer_job(status=StatusJob.PENDING, tentativas=0)
    await repo.salvar(job)

    rodando = Job(
        **{
            **job.__dict__,
            "status": StatusJob.RUNNING,
            "tentativas": 1,
            "iniciado_em": datetime(2026, 4, 16, 11, 0, tzinfo=UTC),
        },
    )
    await repo.salvar(rodando)

    lido = await repo.buscar_por_id(job.id)
    assert lido is not None
    assert lido.status == StatusJob.RUNNING
    assert lido.tentativas == 1
    assert lido.iniciado_em == datetime(2026, 4, 16, 11, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_listar_por_status(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioJobs(async_session)
    await repo.salvar(_fazer_job(status=StatusJob.PENDING))
    await repo.salvar(_fazer_job(status=StatusJob.PENDING))
    await repo.salvar(_fazer_job(status=StatusJob.COMPLETED))

    pendentes = await repo.listar(status=StatusJob.PENDING)
    assert len(pendentes) == 2
    assert all(j.status == StatusJob.PENDING for j in pendentes)
    assert await repo.contar(status=StatusJob.PENDING) == 2
    assert await repo.contar() == 3


@pytest.mark.asyncio
async def test_listar_por_tipo(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioJobs(async_session)
    await repo.salvar(_fazer_job(tipo="processar_cordex"))
    await repo.salvar(_fazer_job(tipo="calcular_pontos_lote"))

    pontos = await repo.listar(tipo="calcular_pontos_lote")
    assert len(pontos) == 1
    assert pontos[0].tipo == "calcular_pontos_lote"
