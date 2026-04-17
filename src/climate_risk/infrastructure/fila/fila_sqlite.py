"""Implementação :class:`FilaSQLite` da porta :class:`FilaJobs`.

ADR-004 (Fila de Jobs em SQLite) + ADR-003 (Timestamps ISO 8601 em TEXT).

**Atomicidade de ``adquirir_proximo``.** A operação é uma instrução
``UPDATE ... WHERE id = (SELECT id ... LIMIT 1) RETURNING *`` única. SQLite
serializa escritas via lock de escrita exclusivo; o SELECT interno é
re-avaliado sob o mesmo snapshot do UPDATE, de forma que dois workers
jamais recebem o mesmo job. A cláusula extra ``AND status='pending'`` no
``UPDATE`` serve como guarda redundante contra qualquer borda residual
(ver :func:`adquirir_proximo` para detalhe).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import JobORM

logger = logging.getLogger(__name__)


class FilaSQLite:
    """Implementação de :class:`FilaJobs` usando SQLAlchemy async + SQLite."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    # ------------------------------------------------------------------
    # Enfileirar
    # ------------------------------------------------------------------
    async def enfileirar(
        self,
        tipo: str,
        payload: dict[str, Any],
        max_tentativas: int = 3,
    ) -> Job:
        agora = utc_now()
        job = Job(
            id=gerar_id("job"),
            tipo=tipo,
            payload=payload,
            status=StatusJob.PENDING,
            tentativas=0,
            max_tentativas=max_tentativas,
            criado_em=agora,
            iniciado_em=None,
            concluido_em=None,
            heartbeat=None,
            erro=None,
            proxima_tentativa_em=None,
        )
        self._sessao.add(_para_orm(job))
        await self._sessao.commit()
        logger.info("Job %s enfileirado (tipo=%s)", job.id, tipo)
        return job

    # ------------------------------------------------------------------
    # Aquisição atômica
    # ------------------------------------------------------------------
    async def adquirir_proximo(self) -> Job | None:
        """Single-statement UPDATE atomico. Ver docstring do módulo."""
        agora_iso = datetime_para_iso(utc_now())

        subq = (
            select(JobORM.id)
            .where(JobORM.status == StatusJob.PENDING)
            .where(
                or_(
                    JobORM.proxima_tentativa_em.is_(None),
                    JobORM.proxima_tentativa_em <= agora_iso,
                )
            )
            .order_by(JobORM.criado_em)
            .limit(1)
            .scalar_subquery()
        )

        stmt = (
            update(JobORM)
            .where(JobORM.id == subq)
            .where(JobORM.status == StatusJob.PENDING)
            .values(
                status=StatusJob.RUNNING,
                iniciado_em=agora_iso,
                heartbeat=agora_iso,
            )
            .returning(JobORM)
            .execution_options(synchronize_session=False)
        )

        resultado = await self._sessao.execute(stmt)
        orm = resultado.scalar_one_or_none()
        await self._sessao.commit()

        if orm is None:
            return None

        job = _para_dominio(orm)
        logger.info("Job %s adquirido (tipo=%s)", job.id, job.tipo)
        return job

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------
    async def atualizar_heartbeat(self, job_id: str) -> None:
        """Atualiza heartbeat somente se o job ainda está em ``running``.

        Uma atualização após transição para estado terminal seria semanticamente
        inválida; o guard ``WHERE status='running'`` evita esse race.
        """
        agora_iso = datetime_para_iso(utc_now())
        stmt = (
            update(JobORM)
            .where(JobORM.id == job_id)
            .where(JobORM.status == StatusJob.RUNNING)
            .values(heartbeat=agora_iso)
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()

    # ------------------------------------------------------------------
    # Conclusão com sucesso
    # ------------------------------------------------------------------
    async def concluir_com_sucesso(self, job_id: str) -> None:
        agora_iso = datetime_para_iso(utc_now())
        stmt = (
            update(JobORM)
            .where(JobORM.id == job_id)
            .values(
                status=StatusJob.COMPLETED,
                concluido_em=agora_iso,
                proxima_tentativa_em=None,
                erro=None,
            )
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()
        logger.info("Job %s concluído com sucesso", job_id)

    # ------------------------------------------------------------------
    # Conclusão com falha (com ou sem retry)
    # ------------------------------------------------------------------
    async def concluir_com_falha(
        self,
        job_id: str,
        erro: str,
        proxima_tentativa_em: datetime | None,
    ) -> None:
        agora_iso = datetime_para_iso(utc_now())
        proxima_iso = datetime_para_iso(proxima_tentativa_em)

        if proxima_tentativa_em is None:
            # Falha terminal.
            stmt = (
                update(JobORM)
                .where(JobORM.id == job_id)
                .values(
                    status=StatusJob.FAILED,
                    concluido_em=agora_iso,
                    erro=erro,
                    proxima_tentativa_em=None,
                    tentativas=JobORM.tentativas + 1,
                )
            )
            await self._sessao.execute(stmt)
            await self._sessao.commit()
            logger.error("Job %s marcado como failed: %s", job_id, erro)
            return

        # Retry agendado: volta para pending com backoff.
        stmt = (
            update(JobORM)
            .where(JobORM.id == job_id)
            .values(
                status=StatusJob.PENDING,
                erro=erro,
                proxima_tentativa_em=proxima_iso,
                tentativas=JobORM.tentativas + 1,
                iniciado_em=None,
                heartbeat=None,
            )
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()
        logger.warning(
            "Job %s voltou para pending após falha (proxima_tentativa=%s): %s",
            job_id,
            proxima_iso,
            erro,
        )

    # ------------------------------------------------------------------
    # Cancelar
    # ------------------------------------------------------------------
    async def cancelar(self, job_id: str) -> bool:
        agora_iso = datetime_para_iso(utc_now())
        stmt = (
            update(JobORM)
            .where(JobORM.id == job_id)
            .where(JobORM.status == StatusJob.PENDING)
            .values(status=StatusJob.CANCELED, concluido_em=agora_iso)
        )
        resultado = await self._sessao.execute(stmt)
        await self._sessao.commit()
        rowcount = getattr(resultado, "rowcount", 0) or 0
        cancelou = rowcount > 0
        if cancelou:
            logger.info("Job %s cancelado", job_id)
        return cancelou

    # ------------------------------------------------------------------
    # Sweep de zumbis
    # ------------------------------------------------------------------
    async def recuperar_zumbis(self, timeout_segundos: int) -> int:
        """Devolve para ``pending`` jobs ``running`` sem heartbeat recente.

        ``tentativas`` é incrementado — a morte do worker conta como uma
        tentativa consumida (evita loops de crash infinitos).
        """
        limite_iso = datetime_para_iso(utc_now() - timedelta(seconds=timeout_segundos))
        stmt = (
            update(JobORM)
            .where(JobORM.status == StatusJob.RUNNING)
            .where(JobORM.heartbeat < limite_iso)
            .values(
                status=StatusJob.PENDING,
                tentativas=JobORM.tentativas + 1,
                proxima_tentativa_em=None,
                iniciado_em=None,
                heartbeat=None,
                erro="recuperado-por-heartbeat-timeout",
            )
        )
        resultado = await self._sessao.execute(stmt)
        await self._sessao.commit()
        recuperados = int(getattr(resultado, "rowcount", 0) or 0)
        if recuperados:
            logger.warning(
                "%d jobs zumbis recuperados (timeout=%ds)",
                recuperados,
                timeout_segundos,
            )
        return int(recuperados)


# ---------------------------------------------------------------------
# Conversores ORM ↔ domínio
# ---------------------------------------------------------------------
def _para_orm(job: Job) -> JobORM:
    criado_em_iso = datetime_para_iso(job.criado_em)
    assert criado_em_iso is not None
    return JobORM(
        id=job.id,
        tipo=job.tipo,
        payload=json.dumps(job.payload),
        status=job.status,
        tentativas=job.tentativas,
        max_tentativas=job.max_tentativas,
        criado_em=criado_em_iso,
        iniciado_em=datetime_para_iso(job.iniciado_em),
        concluido_em=datetime_para_iso(job.concluido_em),
        heartbeat=datetime_para_iso(job.heartbeat),
        erro=job.erro,
        proxima_tentativa_em=datetime_para_iso(job.proxima_tentativa_em),
    )


def _para_dominio(orm: JobORM) -> Job:
    payload: dict[str, Any] = json.loads(orm.payload) if orm.payload else {}
    criado_em = iso_para_datetime(orm.criado_em)
    assert criado_em is not None
    return Job(
        id=orm.id,
        tipo=orm.tipo,
        payload=payload,
        status=orm.status,
        tentativas=orm.tentativas,
        max_tentativas=orm.max_tentativas,
        criado_em=criado_em,
        iniciado_em=iso_para_datetime(orm.iniciado_em),
        concluido_em=iso_para_datetime(orm.concluido_em),
        heartbeat=iso_para_datetime(orm.heartbeat),
        erro=orm.erro,
        proxima_tentativa_em=iso_para_datetime(orm.proxima_tentativa_em),
    )
