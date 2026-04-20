"""Testes e2e de ``GET /admin/stats`` (Slice 12).

O endpoint reúne counters básicos de fornecedores/municípios/jobs/execuções
com os distinct values já expostos por :class:`ConsultarStats`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioFornecedores,
    SQLAlchemyRepositorioJobs,
    SQLAlchemyRepositorioMunicipios,
    SQLAlchemyRepositorioResultados,
)


@pytest.mark.asyncio
async def test_stats_banco_vazio(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.get("/admin/stats")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["contadores"] == {
        "fornecedores": 0,
        "municipios": 0,
        "jobs": 0,
        "execucoes": 0,
    }
    assert corpo["cenarios"] == []
    assert corpo["anos"] == []
    assert corpo["variaveis"] == []
    assert corpo["nomes_indices"] == []
    assert corpo["total_execucoes_com_resultados"] == 0
    assert corpo["total_resultados"] == 0


@pytest.mark.asyncio
async def test_stats_agrega_contadores_e_distincts(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    async with async_sessionmaker_() as sessao:
        repo_forn = SQLAlchemyRepositorioFornecedores(sessao)
        repo_mun = SQLAlchemyRepositorioMunicipios(sessao)
        repo_jobs = SQLAlchemyRepositorioJobs(sessao)
        repo_exec = SQLAlchemyRepositorioExecucoes(sessao)
        repo_res = SQLAlchemyRepositorioResultados(sessao)

        agora = datetime(2026, 4, 20, tzinfo=UTC)

        await repo_mun.salvar_lote(
            [
                Municipio(
                    id=3550308,
                    nome="São Paulo",
                    nome_normalizado="sao paulo",
                    uf="SP",
                    lat_centroide=-23.55,
                    lon_centroide=-46.63,
                    atualizado_em=agora,
                ),
                Municipio(
                    id=3304557,
                    nome="Rio de Janeiro",
                    nome_normalizado="rio de janeiro",
                    uf="RJ",
                    lat_centroide=-22.9,
                    lon_centroide=-43.2,
                    atualizado_em=agora,
                ),
            ]
        )

        await repo_forn.salvar(
            Fornecedor(
                id=gerar_id("forn"),
                nome="Fornecedor X",
                cidade="São Paulo",
                uf="SP",
                criado_em=agora,
                atualizado_em=agora,
            )
        )

        job = Job(
            id=gerar_id("job"),
            tipo="processar_cordex",
            payload={},
            status=StatusJob.PENDING,
            tentativas=0,
            max_tentativas=3,
            criado_em=agora,
            iniciado_em=None,
            concluido_em=None,
            heartbeat=None,
            erro=None,
            proxima_tentativa_em=agora,
        )
        await repo_jobs.salvar(job)

        execucao = Execucao(
            id=gerar_id("exec"),
            cenario="rcp45",
            variavel="pr",
            arquivo_origem="/dados/rcp45.nc",
            tipo="grade_bbox",
            parametros={},
            status=StatusExecucao.COMPLETED,
            criado_em=agora,
            concluido_em=agora,
            job_id=None,
        )
        await repo_exec.salvar(execucao)

        await repo_res.salvar_lote(
            [
                ResultadoIndice(
                    id=gerar_id("res"),
                    execucao_id=execucao.id,
                    lat=-23.5,
                    lon=-46.6,
                    lat_input=-23.5,
                    lon_input=-46.6,
                    ano=2026,
                    nome_indice="PRCPTOT",
                    valor=1200.0,
                    unidade="mm",
                    municipio_id=None,
                ),
                ResultadoIndice(
                    id=gerar_id("res"),
                    execucao_id=execucao.id,
                    lat=-23.5,
                    lon=-46.6,
                    lat_input=-23.5,
                    lon_input=-46.6,
                    ano=2027,
                    nome_indice="CDD",
                    valor=30.0,
                    unidade="dias",
                    municipio_id=None,
                ),
            ]
        )

    resposta = await cliente_api.get("/admin/stats")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["contadores"] == {
        "fornecedores": 1,
        "municipios": 2,
        "jobs": 1,
        "execucoes": 1,
    }
    assert corpo["cenarios"] == ["rcp45"]
    assert sorted(corpo["anos"]) == [2026, 2027]
    assert corpo["variaveis"] == ["pr"]
    assert sorted(corpo["nomes_indices"]) == ["CDD", "PRCPTOT"]
    assert corpo["total_execucoes_com_resultados"] == 1
    assert corpo["total_resultados"] == 2
