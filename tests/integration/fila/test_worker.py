"""Testes de integração do :class:`Worker` com :class:`FilaSQLite`.

O worker é iniciado como task assíncrona. Enfileiramento acontece em outra
sessão (simula: API enfileira, worker consome).
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.application.jobs.handlers_noop import handler_noop
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.modelos import JobORM
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.fila.worker import Worker


async def _aguardar_status(
    sessionmaker: async_sessionmaker[AsyncSession],
    job_id: str,
    esperado: str,
    *,
    timeout: float,
) -> JobORM:
    """Polling até o job atingir o status esperado ou estourar timeout."""
    deadline = asyncio.get_running_loop().time() + timeout
    ultimo: JobORM | None = None
    while asyncio.get_running_loop().time() < deadline:
        async with sessionmaker() as sessao:
            ultimo = await sessao.get(JobORM, job_id)
        if ultimo is not None and ultimo.status == esperado:
            return ultimo
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"Timeout aguardando status={esperado} do job {job_id}; último visto: "
        f"{None if ultimo is None else ultimo.status}"
    )


async def _iniciar_worker(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    poll: float = 0.05,
    heartbeat: float = 0.5,
) -> tuple[Worker, asyncio.Task[None], AsyncSession]:
    """Cria um worker em task dedicada com sessão própria."""
    sessao = sessionmaker()
    sessao_ativa = await sessao.__aenter__()
    fila = FilaSQLite(sessao_ativa)
    worker = Worker(
        fila=fila,
        handlers={"noop": handler_noop},
        poll_interval_seconds=poll,
        heartbeat_seconds=heartbeat,
    )
    task = asyncio.create_task(worker.executar())
    return worker, task, sessao_ativa


async def _parar_worker(
    worker: Worker,
    task: asyncio.Task[None],
    sessao: AsyncSession,
) -> None:
    worker.pedir_encerramento()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    await sessao.close()


# ---------------------------------------------------------------------
# Teste 10 — ciclo completo com handler_noop
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_processa_noop_com_sucesso(
    fila_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with fila_sessionmaker() as sessao_setup:
        fila_setup = FilaSQLite(sessao_setup)
        job = await fila_setup.enfileirar("noop", {"duracao_segundos": 0.05})

    worker, task, sessao_worker = await _iniciar_worker(fila_sessionmaker)
    try:
        orm = await _aguardar_status(fila_sessionmaker, job.id, StatusJob.COMPLETED, timeout=3.0)
        assert orm.erro is None
        assert orm.concluido_em is not None
    finally:
        await _parar_worker(worker, task, sessao_worker)


# ---------------------------------------------------------------------
# Teste 11 — handler que falha (max_tentativas=1 → termina em failed)
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_handler_falha_termina_em_failed(
    fila_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with fila_sessionmaker() as sessao_setup:
        fila_setup = FilaSQLite(sessao_setup)
        job = await fila_setup.enfileirar(
            "noop",
            {"duracao_segundos": 0.0, "falhar": True, "mensagem_erro": "boom"},
            max_tentativas=1,
        )

    worker, task, sessao_worker = await _iniciar_worker(fila_sessionmaker)
    try:
        orm = await _aguardar_status(fila_sessionmaker, job.id, StatusJob.FAILED, timeout=3.0)
        assert orm.erro is not None and "boom" in orm.erro
        assert orm.tentativas == 1
    finally:
        await _parar_worker(worker, task, sessao_worker)


# ---------------------------------------------------------------------
# Teste 12 — múltiplos jobs processados em série
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_processa_varios_jobs(
    fila_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    ids: list[str] = []
    async with fila_sessionmaker() as sessao_setup:
        fila_setup = FilaSQLite(sessao_setup)
        for _ in range(5):
            j = await fila_setup.enfileirar("noop", {"duracao_segundos": 0.02})
            ids.append(j.id)

    worker, task, sessao_worker = await _iniciar_worker(fila_sessionmaker)
    try:
        for job_id in ids:
            await _aguardar_status(fila_sessionmaker, job_id, StatusJob.COMPLETED, timeout=5.0)
    finally:
        await _parar_worker(worker, task, sessao_worker)
