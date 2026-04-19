"""Testes do método :meth:`municipios_com_resultados` (Slice 9)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioResultados,
)


async def _criar_execucao(sessao: AsyncSession) -> str:
    repo = SQLAlchemyRepositorioExecucoes(sessao)
    exec_id = gerar_id("exec")
    await repo.salvar(
        Execucao(
            id=exec_id,
            cenario="rcp45",
            variavel="pr",
            arquivo_origem="/fake/arquivo.nc",
            tipo="grade_bbox",
            parametros={"freq_thr_mm": 20.0},
            status=StatusExecucao.COMPLETED,
            criado_em=datetime(2026, 4, 19, tzinfo=UTC),
            concluido_em=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
            job_id=None,
        )
    )
    return exec_id


def _resultado(execucao_id: str, municipio_id: int | None, ano: int = 2026) -> ResultadoIndice:
    return ResultadoIndice(
        id=gerar_id("res"),
        execucao_id=execucao_id,
        lat=-23.55,
        lon=-46.63,
        lat_input=-23.55,
        lon_input=-46.63,
        ano=ano,
        nome_indice="wet_days",
        valor=110.5,
        unidade="days",
        municipio_id=municipio_id,
    )


@pytest.mark.asyncio
async def test_intersecta_ids(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote(
        [
            _resultado(execucao_id, 1),
            _resultado(execucao_id, 2, ano=2027),
            _resultado(execucao_id, 3),
        ]
    )
    assert await repo.municipios_com_resultados({1, 2, 4}) == {1, 2}


@pytest.mark.asyncio
async def test_entrada_vazia_nao_consulta(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioResultados(async_session)
    assert await repo.municipios_com_resultados(set()) == set()


@pytest.mark.asyncio
async def test_ignora_municipio_id_null(async_session: AsyncSession) -> None:
    """Resultados processados por BBOX (sem geocodificação) têm municipio_id NULL."""
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote(
        [
            _resultado(execucao_id, None),
            _resultado(execucao_id, None, ano=2027),
            _resultado(execucao_id, 10),
        ]
    )
    assert await repo.municipios_com_resultados({10}) == {10}


@pytest.mark.asyncio
async def test_deduplica_ids(async_session: AsyncSession) -> None:
    """Múltiplos resultados para o mesmo município aparecem apenas uma vez."""
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote(
        [
            _resultado(execucao_id, 7, ano=2026),
            _resultado(execucao_id, 7, ano=2027),
            _resultado(execucao_id, 7, ano=2028),
        ]
    )
    assert await repo.municipios_com_resultados({7}) == {7}


@pytest.mark.asyncio
async def test_nenhum_match(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote([_resultado(execucao_id, 1)])
    assert await repo.municipios_com_resultados({99, 100}) == set()
