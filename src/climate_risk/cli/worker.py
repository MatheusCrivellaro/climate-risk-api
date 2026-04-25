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
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.application.calculos.processar_pontos_lote import ProcessarPontosLote
from climate_risk.application.execucoes.processar_cenario import ProcessarCenarioCordex
from climate_risk.application.jobs.handlers_cordex import criar_handler_processar_cordex
from climate_risk.application.jobs.handlers_estresse_hidrico import (
    criar_handler_estresse_hidrico,
    criar_handler_estresse_hidrico_pasta,
)
from climate_risk.application.jobs.handlers_noop import handler_noop
from climate_risk.application.jobs.handlers_pontos import criar_handler_calcular_pontos
from climate_risk.core.config import get_settings
from climate_risk.core.logging import configure_logging
from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.resultado_estresse_hidrico import (
    SQLAlchemyRepositorioResultadoEstresseHidrico,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.fila.worker import Handler, Worker
from climate_risk.infrastructure.leitor_cordex_multi import LeitorCordexMultiVariavel
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray

logger = logging.getLogger(__name__)


def _criar_handler_cordex_com_sessao(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> Handler:
    """Closure que abre uma sessão nova para cada invocação do handler.

    A fila (``FilaSQLite``) usa a sessão do loop do worker para
    ``adquirir_proximo``/``atualizar_heartbeat``. O handler, por sua vez,
    roda em paralelo à task de heartbeat — compartilhar sessão entre
    coroutines concorrentes no SQLAlchemy async é não-seguro. Cada job
    abre uma sessão própria e a fecha ao final.
    """
    leitor = LeitorXarray()

    async def _handler(payload: dict[str, Any]) -> None:
        async with sessionmaker() as sessao:
            repo_execucoes = SQLAlchemyRepositorioExecucoes(sessao)
            repo_resultados = SQLAlchemyRepositorioResultados(sessao)
            caso_uso = ProcessarCenarioCordex(
                leitor_netcdf=leitor,
                repositorio_execucoes=repo_execucoes,
                repositorio_resultados=repo_resultados,
            )
            executor = criar_handler_processar_cordex(caso_uso)
            await executor(payload)

    return _handler


def _criar_handler_pontos_com_sessao(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> Handler:
    """Mesma estratégia do handler CORDEX, aplicada ao lote de pontos (Slice 7)."""
    leitor = LeitorXarray()

    async def _handler(payload: dict[str, Any]) -> None:
        async with sessionmaker() as sessao:
            repo_execucoes = SQLAlchemyRepositorioExecucoes(sessao)
            repo_resultados = SQLAlchemyRepositorioResultados(sessao)
            caso_uso = ProcessarPontosLote(
                leitor_netcdf=leitor,
                repositorio_execucoes=repo_execucoes,
                repositorio_resultados=repo_resultados,
            )
            executor = criar_handler_calcular_pontos(caso_uso)
            await executor(payload)

    return _handler


def _criar_handlers_estresse_hidrico_com_sessao(
    sessionmaker: async_sessionmaker[AsyncSession],
    shapefile_path: str | None,
    cache_dir: str,
) -> tuple[Handler, Handler]:
    """Instância preguiçosa dos handlers de estresse hídrico.

    Compartilha leitor + cache do agregador entre as duas variantes
    (arquivo único e pasta — Slice 17). Motivo: carregar o shapefile
    (~50 MB) e instanciar o agregador é caro — fazemos na **primeira**
    invocação de qualquer um dos handlers. Retorna a dupla
    ``(handler_arquivo, handler_pasta)`` na ordem em que serão registradas.
    """
    leitor = LeitorCordexMultiVariavel()
    agregador_cache: dict[str, AgregadorMunicipiosGeopandas] = {}

    def _obter_agregador() -> AgregadorMunicipiosGeopandas:
        if "instancia" not in agregador_cache:
            if not shapefile_path:
                raise RuntimeError(
                    "shapefile_mun_path não configurado — necessário para "
                    "processar jobs de estresse hídrico."
                )
            from pathlib import Path

            agregador_cache["instancia"] = AgregadorMunicipiosGeopandas(
                shapefile_municipios=Path(shapefile_path),
                cache_dir=Path(cache_dir),
            )
        return agregador_cache["instancia"]

    async def _handler_arquivo(payload: dict[str, Any]) -> None:
        async with sessionmaker() as sessao:
            repo_execucoes = SQLAlchemyRepositorioExecucoes(sessao)
            repo_resultados = SQLAlchemyRepositorioResultadoEstresseHidrico(sessao)
            executor = criar_handler_estresse_hidrico(
                leitor=leitor,
                agregador=_obter_agregador(),
                repositorio_execucoes=repo_execucoes,
                repositorio_resultados=repo_resultados,
            )
            await executor(payload)

    async def _handler_pasta(payload: dict[str, Any]) -> None:
        async with sessionmaker() as sessao:
            repo_execucoes = SQLAlchemyRepositorioExecucoes(sessao)
            repo_resultados = SQLAlchemyRepositorioResultadoEstresseHidrico(sessao)
            executor = criar_handler_estresse_hidrico_pasta(
                leitor=leitor,
                agregador=_obter_agregador(),
                repositorio_execucoes=repo_execucoes,
                repositorio_resultados=repo_resultados,
            )
            await executor(payload)

    return _handler_arquivo, _handler_pasta


async def _rodar_worker() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Inicializando worker com DB=%s", settings.database_url)

    engine = criar_engine(settings.database_url)
    sessionmaker = criar_sessionmaker(engine)

    handler_eh_arquivo, handler_eh_pasta = _criar_handlers_estresse_hidrico_com_sessao(
        sessionmaker,
        settings.shapefile_mun_path,
        settings.cache_dir,
    )
    handlers: dict[str, Handler] = {
        "noop": handler_noop,
        "processar_cordex": _criar_handler_cordex_com_sessao(sessionmaker),
        "calcular_pontos": _criar_handler_pontos_com_sessao(sessionmaker),
        "processar_estresse_hidrico": handler_eh_arquivo,
        "processar_estresse_hidrico_pasta": handler_eh_pasta,
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
