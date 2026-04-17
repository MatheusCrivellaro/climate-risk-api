"""Testes de :class:`SQLAlchemyRepositorioExecucoes`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.infrastructure.db.repositorios import SQLAlchemyRepositorioExecucoes


def _fazer_execucao(
    *,
    id_: str | None = None,
    cenario: str = "rcp45",
    variavel: str = "pr",
    status: str = StatusExecucao.PENDING,
    parametros: dict[str, object] | None = None,
) -> Execucao:
    return Execucao(
        id=id_ or gerar_id("exec"),
        cenario=cenario,
        variavel=variavel,
        arquivo_origem="/dados/pr.nc",
        tipo="grade_bbox",
        parametros=parametros
        or {
            "freq_thr_mm": 20.0,
            "p95_baseline": {"inicio": 2026, "fim": 2030},
            "bbox": {"lat_min": -10.0, "lat_max": 0.0, "lon_min": -50.0, "lon_max": -40.0},
        },
        status=status,
        criado_em=datetime(2026, 4, 16, 10, 30, tzinfo=UTC),
        concluido_em=None,
        job_id=None,
    )


@pytest.mark.asyncio
async def test_persiste_parametros_complexos_como_json(
    async_session: AsyncSession,
) -> None:
    repo = SQLAlchemyRepositorioExecucoes(async_session)
    execucao = _fazer_execucao()
    await repo.salvar(execucao)

    lida = await repo.buscar_por_id(execucao.id)
    assert lida is not None
    assert lida.parametros == execucao.parametros


@pytest.mark.asyncio
async def test_atualizar_status_pending_running_completed(
    async_session: AsyncSession,
) -> None:
    repo = SQLAlchemyRepositorioExecucoes(async_session)
    execucao = _fazer_execucao(status=StatusExecucao.PENDING)
    await repo.salvar(execucao)

    rodando = Execucao(
        **{**execucao.__dict__, "status": StatusExecucao.RUNNING},
    )
    await repo.salvar(rodando)
    assert (await repo.buscar_por_id(execucao.id)).status == StatusExecucao.RUNNING  # type: ignore[union-attr]

    concluida_em = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    concluida = Execucao(
        **{
            **execucao.__dict__,
            "status": StatusExecucao.COMPLETED,
            "concluido_em": concluida_em,
        },
    )
    await repo.salvar(concluida)

    lida = await repo.buscar_por_id(execucao.id)
    assert lida is not None
    assert lida.status == StatusExecucao.COMPLETED
    assert lida.concluido_em == concluida_em


@pytest.mark.asyncio
async def test_listar_com_filtro_combinado_cenario_status(
    async_session: AsyncSession,
) -> None:
    repo = SQLAlchemyRepositorioExecucoes(async_session)
    await repo.salvar(_fazer_execucao(cenario="rcp45", status=StatusExecucao.PENDING))
    await repo.salvar(_fazer_execucao(cenario="rcp45", status=StatusExecucao.COMPLETED))
    await repo.salvar(_fazer_execucao(cenario="rcp85", status=StatusExecucao.COMPLETED))

    rcp45_pending = await repo.listar(cenario="rcp45", status=StatusExecucao.PENDING)
    assert len(rcp45_pending) == 1
    assert rcp45_pending[0].cenario == "rcp45"
    assert rcp45_pending[0].status == StatusExecucao.PENDING

    rcp85 = await repo.listar(cenario="rcp85")
    assert len(rcp85) == 1

    total = await repo.contar()
    assert total == 3
    assert await repo.contar(cenario="rcp45") == 2
    assert await repo.contar(status=StatusExecucao.COMPLETED) == 2
