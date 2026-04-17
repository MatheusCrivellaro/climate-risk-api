"""Worker que consome a fila de jobs (ADR-004, §8.5 do desenho-api).

Loop principal (processo único):

1. Sweep de zumbis (``recuperar_zumbis``).
2. Tenta adquirir próximo job atômicamente (``adquirir_proximo``).
3. Se sem job, dorme ``poll_interval_seconds`` e recomeça.
4. Se pegou job, executa o handler registrado para seu ``tipo``; em paralelo
   mantém uma task de heartbeat a cada ``heartbeat_seconds``.
5. Sucesso → ``concluir_com_sucesso``. Falha → ``concluir_com_falha`` com
   backoff exponencial (2s, 8s, 30s). Atingiu ``max_tentativas`` → marca
   como ``failed`` permanentemente.

Shutdown limpo via SIGTERM/SIGINT: o worker termina o ciclo atual e sai.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.job import Job
from climate_risk.domain.portas.fila_jobs import FilaJobs

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]

_BACKOFF_SEGUNDOS: tuple[int, ...] = (2, 8, 30)


class Worker:
    """Loop de consumo da fila.

    Args:
        fila: Porta :class:`FilaJobs`.
        handlers: Mapa ``tipo -> coroutine`` que recebe o ``payload``.
        poll_interval_seconds: Intervalo entre tentativas quando fila vazia.
        heartbeat_seconds: Intervalo entre atualizações de heartbeat.
        timeout_zumbi_multiplicador: Multiplicador sobre ``heartbeat_seconds``
            usado como janela para considerar um job zumbi (default 3x).
    """

    def __init__(
        self,
        fila: FilaJobs,
        handlers: dict[str, Handler],
        poll_interval_seconds: float,
        heartbeat_seconds: float,
        timeout_zumbi_multiplicador: float = 3.0,
    ) -> None:
        self._fila = fila
        self._handlers = handlers
        self._poll = poll_interval_seconds
        self._heartbeat = heartbeat_seconds
        self._timeout_zumbi = int(heartbeat_seconds * timeout_zumbi_multiplicador)
        self._encerrando = False

    # ------------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------------
    async def executar(self) -> None:
        self._registrar_handlers_sinais()
        logger.info(
            "Worker iniciado (poll=%ss, heartbeat=%ss, timeout_zumbi=%ss). Handlers: %s",
            self._poll,
            self._heartbeat,
            self._timeout_zumbi,
            sorted(self._handlers.keys()),
        )

        while not self._encerrando:
            try:
                await self._fila.recuperar_zumbis(self._timeout_zumbi)

                job = await self._fila.adquirir_proximo()
                if job is None:
                    await self._dormir_poll()
                    continue

                await self._executar_job(job)
            except Exception:
                logger.exception("Erro no loop do worker; aguardando antes de re-tentar")
                await self._dormir_poll()

        logger.info("Worker encerrado.")

    async def _dormir_poll(self) -> None:
        # Sleep cancelável: se SIGTERM chegar durante espera, acorda e sai.
        try:
            await asyncio.sleep(self._poll)
        except asyncio.CancelledError:
            raise

    # ------------------------------------------------------------------
    # Execução de um job
    # ------------------------------------------------------------------
    async def _executar_job(self, job: Job) -> None:
        handler = self._handlers.get(job.tipo)
        if handler is None:
            erro = f"Handler não registrado para tipo '{job.tipo}'"
            logger.error("Job %s: %s", job.id, erro)
            await self._fila.concluir_com_falha(job.id, erro=erro, proxima_tentativa_em=None)
            return

        heartbeat_task = asyncio.create_task(self._loop_heartbeat(job.id))
        try:
            logger.info("Executando job %s (tipo=%s)", job.id, job.tipo)
            await handler(job.payload)
            await self._fila.concluir_com_sucesso(job.id)
        except Exception as erro:
            logger.exception("Falha ao executar job %s", job.id)
            proxima_tentativa_numero = job.tentativas + 1
            if proxima_tentativa_numero >= job.max_tentativas:
                await self._fila.concluir_com_falha(
                    job.id, erro=str(erro), proxima_tentativa_em=None
                )
            else:
                proxima = self._calcular_backoff(proxima_tentativa_numero)
                await self._fila.concluir_com_falha(
                    job.id, erro=str(erro), proxima_tentativa_em=proxima
                )
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _loop_heartbeat(self, job_id: str) -> None:
        while True:
            await asyncio.sleep(self._heartbeat)
            try:
                await self._fila.atualizar_heartbeat(job_id)
            except Exception as erro:
                logger.warning("Falha ao atualizar heartbeat de %s: %s", job_id, erro)

    def _calcular_backoff(self, tentativa: int) -> datetime:
        """Backoff exponencial (2s → 8s → 30s), clamped para ``tentativa >= 3``."""
        idx = min(max(tentativa - 1, 0), len(_BACKOFF_SEGUNDOS) - 1)
        return utc_now() + timedelta(seconds=_BACKOFF_SEGUNDOS[idx])

    # ------------------------------------------------------------------
    # Shutdown limpo
    # ------------------------------------------------------------------
    def _registrar_handlers_sinais(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._sinal_encerramento)
            except NotImplementedError:
                # Windows: loop.add_signal_handler pode não existir.
                signal.signal(sig, lambda *_: self._sinal_encerramento())

    def _sinal_encerramento(self) -> None:
        if not self._encerrando:
            logger.info("Sinal de encerramento recebido. Finalizando ciclo atual...")
            self._encerrando = True

    # ------------------------------------------------------------------
    # Hook para testes: permite encerrar sem sinal.
    # ------------------------------------------------------------------
    def pedir_encerramento(self) -> None:
        self._encerrando = True
