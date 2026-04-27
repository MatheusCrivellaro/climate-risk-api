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
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import JobORM

logger = logging.getLogger(__name__)

# Limite global de tentativas — usado como rede de segurança se algum
# caminho chama ``concluir_com_falha`` com retry mas o job já excedeu o
# limite, e por ``recuperar_zumbis`` para evitar loops de crash infinitos.
MAX_TENTATIVAS: int = 3


class FilaSQLite:
    """Implementação de :class:`FilaJobs` usando SQLAlchemy async + SQLite.

    Args:
        sessao: Sessão usada nos caminhos de leitura e nos writes que NÃO
            competem com a task de heartbeat.
        sessionmaker: Sessão-factory opcional usada nos caminhos terminais
            de escrita (``concluir_com_falha``, ``recuperar_zumbis``) para
            evitar o conflito ``"This session is provisioning a new
            connection; concurrent operations are not permitted"`` quando
            o handler do worker quebra com a heartbeat task ainda viva.
            Quando ``None``, usa-se ``self._sessao`` com rollback prévio.
    """

    def __init__(
        self,
        sessao: AsyncSession,
        sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self._sessao = sessao
        self._sessionmaker = sessionmaker

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
        """Marca falha. Usa sessão limpa quando ``sessionmaker`` foi configurado.

        Inclui rede de segurança: se ``proxima_tentativa_em`` foi pedido mas
        o job já excedeu :data:`MAX_TENTATIVAS`, força status ``failed``
        para evitar loops de re-tentativa.
        """
        agora_iso = datetime_para_iso(utc_now())
        proxima_iso = datetime_para_iso(proxima_tentativa_em)

        async def _executar(sessao: AsyncSession) -> None:
            tentativas_atuais = await self._consultar_tentativas(sessao, job_id)
            forcar_falha_terminal = (
                proxima_tentativa_em is not None and tentativas_atuais + 1 >= MAX_TENTATIVAS
            )

            if proxima_tentativa_em is None or forcar_falha_terminal:
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
                await sessao.execute(stmt)
                await sessao.commit()
                if forcar_falha_terminal:
                    logger.warning(
                        "Job %s atingiu MAX_TENTATIVAS=%d; marcado failed em vez de "
                        "voltar para pending. Erro: %s",
                        job_id,
                        MAX_TENTATIVAS,
                        erro,
                    )
                else:
                    logger.error("Job %s marcado como failed: %s", job_id, erro)
                return

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
            await sessao.execute(stmt)
            await sessao.commit()
            logger.warning(
                "Job %s voltou para pending após falha (proxima_tentativa=%s): %s",
                job_id,
                proxima_iso,
                erro,
            )

        await self._executar_em_sessao_limpa(_executar)

    @staticmethod
    async def _consultar_tentativas(sessao: AsyncSession, job_id: str) -> int:
        """Lê o contador atual de ``tentativas`` do job.

        Retorna 0 se o job não existe (caminho defensivo — não deveria
        acontecer em fluxo normal).
        """
        stmt = select(JobORM.tentativas).where(JobORM.id == job_id)
        resultado = await sessao.execute(stmt)
        valor = resultado.scalar_one_or_none()
        return int(valor) if valor is not None else 0

    async def _executar_em_sessao_limpa(
        self,
        operacao: Any,
    ) -> None:
        """Executa ``operacao(sessao)`` numa sessão isolada.

        Se ``sessionmaker`` foi injetado no construtor, abre uma sessão nova
        para a operação — garantindo isolamento de qualquer transação suja
        deixada pelo handler ou por ``atualizar_heartbeat`` concorrente.

        Sem ``sessionmaker``, faz ``rollback()`` em ``self._sessao`` antes
        de executar — descarta qualquer transação aberta sem fechar e
        previne o erro ``"concurrent operations are not permitted"``.
        """
        if self._sessionmaker is not None:
            async with self._sessionmaker() as sessao:
                await operacao(sessao)
            return

        try:
            await self._sessao.rollback()
        except Exception as erro_rollback:
            logger.warning(
                "Falha ao fazer rollback antes de write terminal: %s",
                erro_rollback,
            )
        await operacao(self._sessao)

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
        tentativa consumida (evita loops de crash infinitos). Zumbis que já
        atingiriam :data:`MAX_TENTATIVAS` após o incremento são marcados
        como ``failed`` em vez de voltar para ``pending``.

        Returns:
            Total de jobs afetados (recuperados para pending **+** marcados
            failed por exceder o limite).
        """
        agora_iso = datetime_para_iso(utc_now())
        limite_iso = datetime_para_iso(utc_now() - timedelta(seconds=timeout_segundos))
        assert agora_iso is not None and limite_iso is not None

        if self._sessionmaker is not None:
            async with self._sessionmaker() as sessao:
                return await self._aplicar_sweep_zumbis(
                    sessao, limite_iso, agora_iso, timeout_segundos
                )

        # Sem sessionmaker: rollback prévio em self._sessao para evitar
        # "concurrent operations" se a sessão veio de um caminho com erro.
        try:
            await self._sessao.rollback()
        except Exception as erro_rollback:
            logger.warning(
                "Falha ao fazer rollback antes de recuperar_zumbis: %s",
                erro_rollback,
            )
        return await self._aplicar_sweep_zumbis(
            self._sessao, limite_iso, agora_iso, timeout_segundos
        )

    @staticmethod
    async def _aplicar_sweep_zumbis(
        sessao: AsyncSession,
        limite_iso: str,
        agora_iso: str,
        timeout_segundos: int,
    ) -> int:
        falha_stmt = (
            update(JobORM)
            .where(JobORM.status == StatusJob.RUNNING)
            .where(JobORM.heartbeat < limite_iso)
            .where(JobORM.tentativas + 1 >= MAX_TENTATIVAS)
            .values(
                status=StatusJob.FAILED,
                concluido_em=agora_iso,
                tentativas=JobORM.tentativas + 1,
                proxima_tentativa_em=None,
                iniciado_em=None,
                heartbeat=None,
                erro="recuperado-por-heartbeat-timeout (limite atingido)",
            )
        )
        res_falha = await sessao.execute(falha_stmt)
        falhados = int(getattr(res_falha, "rowcount", 0) or 0)

        pending_stmt = (
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
        res_pending = await sessao.execute(pending_stmt)
        recuperados = int(getattr(res_pending, "rowcount", 0) or 0)

        await sessao.commit()

        if falhados:
            logger.warning(
                "%d jobs zumbis marcados FAILED por exceder MAX_TENTATIVAS=%d",
                falhados,
                MAX_TENTATIVAS,
            )
        if recuperados:
            logger.warning(
                "%d jobs zumbis recuperados (timeout=%ds)",
                recuperados,
                timeout_segundos,
            )
        return falhados + recuperados


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
