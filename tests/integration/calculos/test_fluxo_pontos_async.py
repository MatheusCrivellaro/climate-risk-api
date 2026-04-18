"""Integração do fluxo assíncrono ``POST /calculos/pontos`` (Slice 7).

Fluxo completo sem worker em loop:

1. :class:`CriarExecucaoPorPontos` cria ``pending`` + enfileira job.
2. Chamamos o handler diretamente (simulando o consumo do Worker) —
   evita flakiness com polling/tempo.
3. Verificamos a :class:`Execucao` em ``completed`` + :class:`ResultadoIndice`
   persistidos com ``execucao_id`` correto.

Repositórios e fila são reais (SQLAlchemy + SQLite in-memory).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.application.calculos.calcular_por_pontos import (
    UNIDADES_POR_INDICE,
    PontoEntradaDominio,
)
from climate_risk.application.calculos.criar_execucao_por_pontos import (
    CriarExecucaoPorPontos,
    ParametrosCriacaoExecucaoPontos,
)
from climate_risk.application.calculos.processar_pontos_lote import ProcessarPontosLote
from climate_risk.application.jobs.handlers_pontos import criar_handler_calcular_pontos
from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.infrastructure.db.modelos import ExecucaoORM, JobORM, ResultadoIndiceORM
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray

FIXTURE_NC = (
    Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini" / "cordex_sintetico_basico.nc"
)


def _pontos(n: int) -> list[PontoEntradaDominio]:
    return [
        PontoEntradaDominio(lat=-22.9 + 0.001 * i, lon=-46.5, identificador=f"P{i:04d}")
        for i in range(n)
    ]


@pytest.mark.skipif(not FIXTURE_NC.exists(), reason="Fixture sintética básica ausente.")
@pytest.mark.asyncio
async def test_criar_e_processar_lote_ponta_a_ponta(async_session: AsyncSession) -> None:
    repo_execucoes = SQLAlchemyRepositorioExecucoes(async_session)
    repo_resultados = SQLAlchemyRepositorioResultados(async_session)
    fila = FilaSQLite(async_session)

    # Fase 1: criação síncrona — Execucao pending + Job enfileirado.
    criar = CriarExecucaoPorPontos(repositorio_execucoes=repo_execucoes, fila_jobs=fila)
    params = ParametrosCriacaoExecucaoPontos(
        arquivo_nc=str(FIXTURE_NC),
        cenario="rcp45",
        variavel="pr",
        pontos=_pontos(3),
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2030),
        p95_wet_thr=1.0,
    )
    criacao = await criar.executar(params)
    await async_session.commit()
    async_session.expire_all()

    execucao_pending = await async_session.get(ExecucaoORM, criacao.execucao_id)
    assert execucao_pending is not None
    assert execucao_pending.status == StatusExecucao.PENDING
    assert execucao_pending.tipo == "pontos_lote"
    assert execucao_pending.job_id == criacao.job_id

    job = await async_session.get(JobORM, criacao.job_id)
    assert job is not None
    assert job.tipo == "calcular_pontos"
    payload = json.loads(job.payload)

    # Fase 2: handler consome (sem Worker loop — simulamos consumo direto).
    processar = ProcessarPontosLote(
        leitor_netcdf=LeitorXarray(),
        repositorio_execucoes=repo_execucoes,
        repositorio_resultados=repo_resultados,
    )
    handler = criar_handler_calcular_pontos(processar)
    await handler(payload)
    await async_session.commit()
    async_session.expire_all()

    # Fase 3: execução virou completed + resultados gravados com execucao_id certo.
    execucao_final = await async_session.get(ExecucaoORM, criacao.execucao_id)
    assert execucao_final is not None
    assert execucao_final.status == StatusExecucao.COMPLETED
    assert execucao_final.concluido_em is not None

    resultados = (
        (
            await async_session.execute(
                select(ResultadoIndiceORM).where(
                    ResultadoIndiceORM.execucao_id == criacao.execucao_id
                )
            )
        )
        .scalars()
        .all()
    )
    # 3 pontos x 5 anos (fixture basica) x 8 indices = 120 linhas.
    assert len(resultados) == 3 * 5 * 8
    for r in resultados:
        assert r.execucao_id == criacao.execucao_id
        assert r.unidade == UNIDADES_POR_INDICE[r.nome_indice]
        # Slice 7 preserva lat_input/lon_input originais do usuário.
        assert r.lat_input is not None
        assert r.lon_input is not None


@pytest.mark.skipif(not FIXTURE_NC.exists(), reason="Fixture sintética básica ausente.")
@pytest.mark.asyncio
async def test_handler_com_payload_falhando_marca_execucao_failed(
    async_session: AsyncSession,
) -> None:
    repo_execucoes = SQLAlchemyRepositorioExecucoes(async_session)
    repo_resultados = SQLAlchemyRepositorioResultados(async_session)
    fila = FilaSQLite(async_session)

    # Cria execução + job normalmente.
    criar = CriarExecucaoPorPontos(repositorio_execucoes=repo_execucoes, fila_jobs=fila)
    params = ParametrosCriacaoExecucaoPontos(
        arquivo_nc=str(FIXTURE_NC),
        cenario="rcp45",
        variavel="pr",
        pontos=_pontos(2),
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2030),
        p95_wet_thr=1.0,
    )
    criacao = await criar.executar(params)
    await async_session.commit()
    async_session.expire_all()

    # Reescreve o payload com arquivo_nc inválido — leitor vai falhar.
    job = await async_session.get(JobORM, criacao.job_id)
    assert job is not None
    payload_corrompido = json.loads(job.payload)
    payload_corrompido["arquivo_nc"] = "/nao/existe.nc"

    processar = ProcessarPontosLote(
        leitor_netcdf=LeitorXarray(),
        repositorio_execucoes=repo_execucoes,
        repositorio_resultados=repo_resultados,
    )
    handler = criar_handler_calcular_pontos(processar)

    with pytest.raises(ErroArquivoNCNaoEncontrado):
        await handler(payload_corrompido)
    await async_session.commit()
    async_session.expire_all()

    execucao_final = await async_session.get(ExecucaoORM, criacao.execucao_id)
    assert execucao_final is not None
    assert execucao_final.status == StatusExecucao.FAILED
    assert execucao_final.concluido_em is not None

    # Nenhum resultado persistido.
    resultados = (
        (
            await async_session.execute(
                select(ResultadoIndiceORM).where(
                    ResultadoIndiceORM.execucao_id == criacao.execucao_id
                )
            )
        )
        .scalars()
        .all()
    )
    assert resultados == []

    # Job continua pending — a fila ainda não processou (o teste simulou).
    job_final = await async_session.get(JobORM, criacao.job_id)
    assert job_final is not None
    assert job_final.status == StatusJob.PENDING
