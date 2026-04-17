"""Testes de integração de :class:`FilaSQLite`.

Cobrem contratos da porta :class:`FilaJobs` contra SQLite real em arquivo,
com ênfase em **atomicidade** da aquisição (Teste 3).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.conversores_tempo import datetime_para_iso
from climate_risk.infrastructure.db.modelos import JobORM
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite


# ---------------------------------------------------------------------
# Teste 1 — enfileirar e ordem por criado_em
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_enfileirar_persiste_em_ordem(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)

    j1 = await fila.enfileirar("noop", {"n": 1})
    j2 = await fila.enfileirar("noop", {"n": 2})
    j3 = await fila.enfileirar("noop", {"n": 3})

    res = await fila_sessao.execute(select(JobORM).order_by(JobORM.criado_em))
    ordem = [o.id for o in res.scalars().all()]
    assert ordem == [j1.id, j2.id, j3.id]

    # Aquisição também respeita criado_em (FIFO).
    a = await fila.adquirir_proximo()
    assert a is not None and a.id == j1.id


# ---------------------------------------------------------------------
# Teste 2 — aquisição básica
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_adquirir_retorna_job_e_marca_running(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    criado = await fila.enfileirar("noop", {})

    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None
    assert adquirido.id == criado.id
    assert adquirido.status == StatusJob.RUNNING
    assert adquirido.iniciado_em is not None
    assert adquirido.heartbeat is not None

    proximo = await fila.adquirir_proximo()
    assert proximo is None


# ---------------------------------------------------------------------
# Teste 3 — ATOMICIDADE: dois workers concorrentes, só um adquire.
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_atomicidade_adquirir_proximo_dois_workers(
    fila_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    # Enfileira em uma sessão; depois simula dois workers.
    async with fila_sessionmaker() as sessao_setup:
        fila_setup = FilaSQLite(sessao_setup)
        await fila_setup.enfileirar("noop", {"único": True})

    async def adquirir_em_sessao_nova() -> str | None:
        async with fila_sessionmaker() as sessao:
            fila = FilaSQLite(sessao)
            job = await fila.adquirir_proximo()
            return job.id if job else None

    # asyncio.gather dispara as duas corrotinas "em paralelo";
    # SQLite serializa writes via lock.
    resultados = await asyncio.gather(
        adquirir_em_sessao_nova(),
        adquirir_em_sessao_nova(),
    )
    vencedores = [r for r in resultados if r is not None]
    perdedores = [r for r in resultados if r is None]
    assert len(vencedores) == 1, f"Esperado exatamente 1 vencedor, obtido {resultados}"
    assert len(perdedores) == 1


# ---------------------------------------------------------------------
# Teste 4 — heartbeat
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_atualizar_heartbeat_atualiza_campo(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    await fila.enfileirar("noop", {})
    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None
    heartbeat_inicial = adquirido.heartbeat

    # pequenas esperas garantem timestamp diferente
    await asyncio.sleep(0.01)
    await fila.atualizar_heartbeat(adquirido.id)

    orm = await fila_sessao.get(JobORM, adquirido.id)
    assert orm is not None
    assert orm.heartbeat is not None
    assert heartbeat_inicial is not None
    assert orm.heartbeat >= datetime_para_iso(heartbeat_inicial)  # type: ignore[operator]


# ---------------------------------------------------------------------
# Teste 5 — conclusão com sucesso
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concluir_com_sucesso(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    await fila.enfileirar("noop", {})
    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None

    await fila.concluir_com_sucesso(adquirido.id)
    orm = await fila_sessao.get(JobORM, adquirido.id)
    assert orm is not None
    assert orm.status == StatusJob.COMPLETED
    assert orm.concluido_em is not None
    assert orm.erro is None


# ---------------------------------------------------------------------
# Teste 6 — falha com retry (volta para pending com backoff)
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_falha_com_backoff_volta_para_pending(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    await fila.enfileirar("noop", {}, max_tentativas=3)
    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None

    proxima = utc_now().replace(microsecond=0) + _seg(3600)  # bem no futuro
    await fila.concluir_com_falha(adquirido.id, erro="boom", proxima_tentativa_em=proxima)

    orm = await fila_sessao.get(JobORM, adquirido.id)
    assert orm is not None
    assert orm.status == StatusJob.PENDING
    assert orm.tentativas == 1
    assert orm.erro == "boom"
    assert orm.proxima_tentativa_em is not None

    # Não deve ser adquirido (proxima_tentativa_em no futuro).
    nenhum = await fila.adquirir_proximo()
    assert nenhum is None


# ---------------------------------------------------------------------
# Teste 7 — falha terminal (proxima_tentativa_em=None ⇒ failed)
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_falha_terminal_marca_failed(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    await fila.enfileirar("noop", {}, max_tentativas=2)
    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None

    await fila.concluir_com_falha(
        adquirido.id, erro="sem mais tentativas", proxima_tentativa_em=None
    )

    orm = await fila_sessao.get(JobORM, adquirido.id)
    assert orm is not None
    assert orm.status == StatusJob.FAILED
    assert orm.erro == "sem mais tentativas"
    assert orm.concluido_em is not None
    assert orm.tentativas == 1  # incrementou


# ---------------------------------------------------------------------
# Teste 8 — cancelar só funciona em pending
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cancelar_pending_retorna_true(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    job = await fila.enfileirar("noop", {})

    ok = await fila.cancelar(job.id)
    assert ok is True

    orm = await fila_sessao.get(JobORM, job.id)
    assert orm is not None
    assert orm.status == StatusJob.CANCELED


@pytest.mark.asyncio
async def test_cancelar_running_retorna_false(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    job = await fila.enfileirar("noop", {})
    await fila.adquirir_proximo()

    ok = await fila.cancelar(job.id)
    assert ok is False

    orm = await fila_sessao.get(JobORM, job.id)
    assert orm is not None
    assert orm.status == StatusJob.RUNNING


# ---------------------------------------------------------------------
# Teste 9 — recuperar zumbis
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_recuperar_zumbis_devolve_para_pending(fila_sessao: AsyncSession) -> None:
    fila = FilaSQLite(fila_sessao)
    await fila.enfileirar("noop", {})
    adquirido = await fila.adquirir_proximo()
    assert adquirido is not None

    # Força heartbeat "antigo" (5 minutos atrás).
    heartbeat_antigo = datetime_para_iso(utc_now() - _seg(300))
    await fila_sessao.execute(
        update(JobORM).where(JobORM.id == adquirido.id).values(heartbeat=heartbeat_antigo)
    )
    await fila_sessao.commit()

    recuperados = await fila.recuperar_zumbis(timeout_segundos=60)
    assert recuperados == 1

    orm = await fila_sessao.get(JobORM, adquirido.id)
    assert orm is not None
    assert orm.status == StatusJob.PENDING
    assert orm.tentativas == 1  # a morte conta como tentativa
    assert orm.heartbeat is None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _seg(n: int):  # type: ignore[no-untyped-def]
    from datetime import timedelta

    return timedelta(seconds=n)


# Silencia linters que exigem algum uso de AsyncIterator importado.
_: AsyncIterator[int] | None = None
