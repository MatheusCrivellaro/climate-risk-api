"""Teste e2e do fluxo completo CORDEX (UC-02).

1. Cliente HTTP chama ``POST /execucoes`` (202 Accepted).
2. Um ``Worker`` in-process (mesmo engine/sessionmaker) consome a fila.
3. Aguardamos a :class:`Execucao` chegar a ``completed`` via polling.
4. Verificamos :class:`ResultadoIndice` persistidos e o :class:`Job` em
   ``completed``.

Usa SQLite em **arquivo temporário** (não ``:memory:``) para garantir
que múltiplas sessões concorrentes (API + worker) enxerguem o mesmo
banco — mesma estratégia dos testes de integração da fila.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import climate_risk.infrastructure.db.modelos  # noqa: F401 — popula Base.metadata
from climate_risk.application.execucoes.processar_cenario import ProcessarCenarioCordex
from climate_risk.application.jobs.handlers_cordex import criar_handler_processar_cordex
from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.base import Base
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker
from climate_risk.infrastructure.db.modelos import (
    ExecucaoORM,
    JobORM,
    ResultadoIndiceORM,
)
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.fila.worker import Handler, Worker
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray
from climate_risk.interfaces.app import create_app

FIXTURE_NC = (
    Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini" / "cordex_sintetico_basico.nc"
)


@pytest_asyncio.fixture
async def fluxo_engine(tmp_path: Path) -> AsyncGenerator[AsyncEngine, None]:
    db_path = tmp_path / "fluxo_cordex.db"
    engine = criar_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def fluxo_sessionmaker(
    fluxo_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return criar_sessionmaker(fluxo_engine)


@pytest_asyncio.fixture
async def fluxo_cliente(
    fluxo_sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    app = create_app()

    async def _get_sessao_teste() -> AsyncGenerator[AsyncSession, None]:
        async with fluxo_sessionmaker() as sessao:
            yield sessao

    app.dependency_overrides[get_sessao] = _get_sessao_teste

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as cliente:
        yield cliente


def _corpo_bbox_pequeno() -> dict[str, object]:
    """Mantém o teste rápido restringindo a 1x1 célula."""
    return {
        "arquivo_nc": str(FIXTURE_NC),
        "cenario": "rcp45",
        "variavel": "pr",
        "bbox": {"lat_min": -23.1, "lat_max": -22.9, "lon_min": -47.1, "lon_max": -46.9},
        "parametros_indices": {
            "freq_thr_mm": 20.0,
            "p95_wet_thr": 1.0,
            "heavy20": 20.0,
            "heavy50": 50.0,
            "p95_baseline": {"inicio": 2026, "fim": 2030},
        },
    }


async def _iniciar_worker(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> tuple[Worker, asyncio.Task[None], AsyncSession]:
    """Worker in-process compartilhando o mesmo engine dos testes e2e."""
    sessao = sessionmaker()
    sessao_ativa = await sessao.__aenter__()

    leitor = LeitorXarray()

    async def _handler_factory_payload(payload: dict[str, object]) -> None:
        # Cada invocação do handler abre sua própria sessão (igual ao CLI).
        async with sessionmaker() as s:
            repo_execucoes = SQLAlchemyRepositorioExecucoes(s)
            repo_resultados = SQLAlchemyRepositorioResultados(s)
            caso = ProcessarCenarioCordex(
                leitor_netcdf=leitor,
                repositorio_execucoes=repo_execucoes,
                repositorio_resultados=repo_resultados,
            )
            executor = criar_handler_processar_cordex(caso)
            await executor(payload)

    fila = FilaSQLite(sessao_ativa)
    handlers: dict[str, Handler] = {"processar_cordex": _handler_factory_payload}
    worker = Worker(
        fila=fila,
        handlers=handlers,
        poll_interval_seconds=0.05,
        heartbeat_seconds=0.5,
    )
    task = asyncio.create_task(worker.executar())
    return worker, task, sessao_ativa


async def _parar_worker(worker: Worker, task: asyncio.Task[None], sessao: AsyncSession) -> None:
    worker.pedir_encerramento()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    await sessao.close()


async def _aguardar_status_execucao(
    sessionmaker: async_sessionmaker[AsyncSession],
    execucao_id: str,
    esperado: str,
    *,
    timeout: float,
) -> ExecucaoORM:
    deadline = asyncio.get_running_loop().time() + timeout
    ultimo: ExecucaoORM | None = None
    while asyncio.get_running_loop().time() < deadline:
        async with sessionmaker() as sessao:
            ultimo = await sessao.get(ExecucaoORM, execucao_id)
        if ultimo is not None and ultimo.status == esperado:
            return ultimo
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"Timeout aguardando status={esperado} para {execucao_id}; "
        f"último={None if ultimo is None else ultimo.status}"
    )


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.asyncio
async def test_fluxo_cordex_completo_api_para_worker(
    fluxo_cliente: AsyncClient,
    fluxo_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    # Fase 1: API enfileira.
    resposta = await fluxo_cliente.post("/api/execucoes", json=_corpo_bbox_pequeno())
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    execucao_id = corpo["execucao_id"]
    job_id = corpo["job_id"]

    # Fase 2: worker in-process consome.
    worker, task, sessao_worker = await _iniciar_worker(fluxo_sessionmaker)
    try:
        completed = await _aguardar_status_execucao(
            fluxo_sessionmaker, execucao_id, StatusExecucao.COMPLETED, timeout=30.0
        )
        assert completed.concluido_em is not None
    finally:
        await _parar_worker(worker, task, sessao_worker)

    # Fase 3: verifica persistência de resultados + job completed.
    async with fluxo_sessionmaker() as sessao:
        resultados = (
            (
                await sessao.execute(
                    select(ResultadoIndiceORM).where(ResultadoIndiceORM.execucao_id == execucao_id)
                )
            )
            .scalars()
            .all()
        )
        # 1 celula x 5 anos x 8 indices = 40 linhas.
        assert len(resultados) == 40

        job = (await sessao.execute(select(JobORM).where(JobORM.id == job_id))).scalar_one()
        assert job.status == StatusJob.COMPLETED
