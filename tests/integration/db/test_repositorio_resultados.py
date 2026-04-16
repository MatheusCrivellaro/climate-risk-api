"""Testes de :class:`SQLAlchemyRepositorioResultados`."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioResultados,
)


async def _criar_execucao(sessao: AsyncSession, cenario: str = "rcp45") -> str:
    repo = SQLAlchemyRepositorioExecucoes(sessao)
    execucao = Execucao(
        id=gerar_id("exec"),
        cenario=cenario,
        variavel="pr",
        arquivo_origem="/dados/pr.nc",
        tipo="grade_bbox",
        parametros={},
        status=StatusExecucao.COMPLETED,
        criado_em=datetime(2026, 4, 16, tzinfo=UTC),
        concluido_em=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        job_id=None,
    )
    await repo.salvar(execucao)
    return execucao.id


def _fazer_resultado(
    *,
    execucao_id: str,
    lat: float = -23.5,
    lon: float = -46.6,
    ano: int = 2030,
    nome_indice: str = "wet_days",
    valor: float | None = 50.0,
    unidade: str = "dias",
) -> ResultadoIndice:
    return ResultadoIndice(
        id=gerar_id("res"),
        execucao_id=execucao_id,
        lat=lat,
        lon=lon,
        lat_input=None,
        lon_input=None,
        ano=ano,
        nome_indice=nome_indice,
        valor=valor,
        unidade=unidade,
        municipio_id=None,
    )


@pytest.mark.asyncio
async def test_salvar_lote_e_listar_por_execucao(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    resultados = [
        _fazer_resultado(execucao_id=execucao_id, ano=ano, nome_indice=nome)
        for ano in (2026, 2027, 2028, 2029, 2030)
        for nome in ("wet_days", "sdii", "rx1day", "rx5day")
    ]
    assert len(resultados) == 20
    await repo.salvar_lote(resultados)

    total = await repo.contar(execucao_id=execucao_id)
    assert total == 20
    lidos = await repo.listar(execucao_id=execucao_id, limit=200)
    assert len(lidos) == 20


@pytest.mark.asyncio
async def test_filtrar_por_ano_min_e_ano_max(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    for ano in (2026, 2027, 2028, 2029, 2030):
        await repo.salvar_lote([_fazer_resultado(execucao_id=execucao_id, ano=ano)])

    entre_2027_2029 = await repo.listar(execucao_id=execucao_id, ano_min=2027, ano_max=2029)
    assert sorted(r.ano for r in entre_2027_2029) == [2027, 2028, 2029]


@pytest.mark.asyncio
async def test_filtrar_por_nome_indice(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    for nome in ("wet_days", "sdii", "rx1day"):
        await repo.salvar_lote([_fazer_resultado(execucao_id=execucao_id, nome_indice=nome)])

    lidos = await repo.listar(execucao_id=execucao_id, nome_indice="sdii")
    assert len(lidos) == 1
    assert lidos[0].nome_indice == "sdii"


@pytest.mark.asyncio
async def test_filtrar_por_bbox_retorna_apenas_pontos_dentro(
    async_session: AsyncSession,
) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    pontos = [
        _fazer_resultado(execucao_id=execucao_id, lat=-20.0, lon=-45.0),
        _fazer_resultado(execucao_id=execucao_id, lat=-22.0, lon=-46.0),
        _fazer_resultado(execucao_id=execucao_id, lat=0.0, lon=0.0),
        _fazer_resultado(execucao_id=execucao_id, lat=-30.0, lon=-60.0),
    ]
    await repo.salvar_lote(pontos)

    bbox = BoundingBox(lat_min=-25.0, lat_max=-15.0, lon_min=-50.0, lon_max=-40.0)
    lidos = await repo.listar(execucao_id=execucao_id, bbox=bbox)
    assert len(lidos) == 2
    for r in lidos:
        assert -25.0 <= r.lat <= -15.0
        assert -50.0 <= r.lon <= -40.0


@pytest.mark.asyncio
async def test_filtrar_por_bbox_cruzando_antimeridiano(
    async_session: AsyncSession,
) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    pontos = [
        _fazer_resultado(execucao_id=execucao_id, lat=0.0, lon=175.0),
        _fazer_resultado(execucao_id=execucao_id, lat=0.0, lon=-175.0),
        _fazer_resultado(execucao_id=execucao_id, lat=0.0, lon=0.0),
    ]
    await repo.salvar_lote(pontos)

    bbox = BoundingBox(lat_min=-10.0, lat_max=10.0, lon_min=170.0, lon_max=-170.0)
    lidos = await repo.listar(execucao_id=execucao_id, bbox=bbox)
    assert sorted(r.lon for r in lidos) == [-175.0, 175.0]


@pytest.mark.asyncio
async def test_persiste_valor_none_como_null(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    resultado = _fazer_resultado(execucao_id=execucao_id, valor=None)
    await repo.salvar_lote([resultado])

    lidos = await repo.listar(execucao_id=execucao_id)
    assert len(lidos) == 1
    assert lidos[0].valor is None


@pytest.mark.asyncio
async def test_salvar_lote_vazio_noop(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote([])
    assert await repo.contar(execucao_id=execucao_id) == 0


@pytest.mark.asyncio
async def test_filtrar_por_cenario_via_join(async_session: AsyncSession) -> None:
    exec_rcp45 = await _criar_execucao(async_session, cenario="rcp45")
    exec_rcp85 = await _criar_execucao(async_session, cenario="rcp85")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _fazer_resultado(execucao_id=exec_rcp45),
            _fazer_resultado(execucao_id=exec_rcp45),
            _fazer_resultado(execucao_id=exec_rcp85),
        ]
    )

    assert await repo.contar(cenario="rcp45") == 2
    assert await repo.contar(cenario="rcp85") == 1


@pytest.mark.asyncio
async def test_performance_smoke_1000_resultados(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    resultados = [
        _fazer_resultado(execucao_id=execucao_id, ano=2026 + (i % 5)) for i in range(1000)
    ]
    inicio = time.perf_counter()
    await repo.salvar_lote(resultados)
    decorrido = time.perf_counter() - inicio

    assert await repo.contar(execucao_id=execucao_id) == 1000
    assert decorrido < 5.0, f"salvar_lote(1000) levou {decorrido:.2f}s (> 5.0s)"
