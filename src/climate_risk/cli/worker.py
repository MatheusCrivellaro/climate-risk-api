"""Entry-point CLI do worker.

Monta as dependências concretas (engine SQLite + :class:`FilaSQLite` +
handlers) e roda o :class:`Worker` em loop infinito até receber SIGTERM.

Uso::

    uv run climate-risk-worker
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys

from climate_risk.application.jobs.handlers_noop import handler_noop
from climate_risk.core.config import get_settings
from climate_risk.core.logging import configure_logging
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.fila.worker import Handler, Worker

logger = logging.getLogger(__name__)


async def _rodar_worker() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Inicializando worker com DB=%s", settings.database_url)

    engine = criar_engine(settings.database_url)
    sessionmaker = criar_sessionmaker(engine)

    handlers: dict[str, Handler] = {
        "noop": handler_noop,
        # Slice 6 adicionará "processar_cordex": handler_processar_cordex.
    }

    try:
        async with sessionmaker() as sessao:
            fila = FilaSQLite(sessao)
            worker = Worker(
                fila=fila,
                handlers=handlers,
                poll_interval_seconds=float(settings.worker_poll_interval_seconds),
                heartbeat_seconds=float(settings.worker_heartbeat_seconds),
            )
            await worker.executar()
    finally:
        await engine.dispose()


def main() -> int:
    """Entry point para o script ``climate-risk-worker``."""
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_rodar_worker())
    return 0


if __name__ == "__main__":
    sys.exit(main())
