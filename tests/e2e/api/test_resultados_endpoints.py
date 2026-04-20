"""Testes e2e dos endpoints ``/resultados`` (Slice 11)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioResultados,
)


async def _popular(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> tuple[str, str]:
    """Insere 2 execuções + resultados variados. Retorna (exec_45, exec_85)."""
    async with async_sessionmaker_() as sessao:
        repo_exec = SQLAlchemyRepositorioExecucoes(sessao)
        repo_res = SQLAlchemyRepositorioResultados(sessao)

        exec_45 = Execucao(
            id=gerar_id("exec"),
            cenario="rcp45",
            variavel="pr",
            arquivo_origem="/dados/rcp45.nc",
            tipo="grade_bbox",
            parametros={},
            status=StatusExecucao.COMPLETED,
            criado_em=datetime(2026, 4, 16, tzinfo=UTC),
            concluido_em=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
            job_id=None,
        )
        exec_85 = Execucao(
            id=gerar_id("exec"),
            cenario="rcp85",
            variavel="pr",
            arquivo_origem="/dados/rcp85.nc",
            tipo="grade_bbox",
            parametros={},
            status=StatusExecucao.COMPLETED,
            criado_em=datetime(2026, 4, 17, tzinfo=UTC),
            concluido_em=datetime(2026, 4, 17, 12, 0, tzinfo=UTC),
            job_id=None,
        )
        await repo_exec.salvar(exec_45)
        await repo_exec.salvar(exec_85)

        dados = [
            (exec_45.id, -23.5, -46.6, 2026, "PRCPTOT", 1200.0),
            (exec_45.id, -23.5, -46.6, 2027, "PRCPTOT", 1250.0),
            (exec_45.id, -22.9, -43.2, 2026, "PRCPTOT", 1000.0),
            (exec_45.id, -22.9, -43.2, 2026, "CDD", 30.0),
            (exec_85.id, -23.5, -46.6, 2026, "PRCPTOT", 1300.0),
            (exec_85.id, -23.5, -46.6, 2026, "CDD", 45.0),
        ]
        await repo_res.salvar_lote(
            [
                ResultadoIndice(
                    id=gerar_id("res"),
                    execucao_id=exec_id,
                    lat=lat,
                    lon=lon,
                    lat_input=lat,
                    lon_input=lon,
                    ano=ano,
                    nome_indice=nome,
                    valor=valor,
                    unidade="mm",
                    municipio_id=None,
                )
                for exec_id, lat, lon, ano, nome, valor in dados
            ]
        )
        return exec_45.id, exec_85.id


@pytest.mark.asyncio
async def test_get_resultados_lista_todos_por_padrao(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 6
    assert len(body["items"]) == 6


@pytest.mark.asyncio
async def test_get_resultados_filtra_por_cenario(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados", params={"cenario": "rcp85"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2


@pytest.mark.asyncio
async def test_get_resultados_filtra_por_nomes_indices_csv(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados", params={"nomes_indices": "CDD"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert all(i["nome_indice"] == "CDD" for i in body["items"])


@pytest.mark.asyncio
async def test_get_resultados_paginacao(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados", params={"limit": 2, "offset": 0})
    body = resp.json()
    assert body["total"] == 6
    assert len(body["items"]) == 2


@pytest.mark.asyncio
async def test_get_resultados_raio_km_exige_centros(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados", params={"raio_km": 100, "centro_lat": -23.5})
    assert resp.status_code == 422
    body = resp.json()
    assert body["title"] == "Parâmetros inválidos"


@pytest.mark.asyncio
async def test_get_resultados_com_raio_filtra_por_haversine(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    # Centro em SP (-23.5,-46.6). Raio 100 km cobre só SP (~0 km), não Rio (~358 km).
    resp = await cliente_api.get(
        "/api/resultados",
        params={
            "raio_km": 100,
            "centro_lat": -23.5,
            "centro_lon": -46.6,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    # Todos os pontos em SP: 3 do rcp45 (2 anos PRCPTOT) + rcp85 (2: PRCPTOT+CDD)
    # = 3 do rcp45 (ano 2026, 2027 PRCPTOT) + 2 do rcp85 = 4 no total (sp).
    assert body["total"] == 4
    assert all(abs(i["lat"] + 23.5) < 0.1 for i in body["items"])


@pytest.mark.asyncio
async def test_get_agregados_media_por_ano(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get(
        "/api/resultados/agregados",
        params={"agregacao": "media", "agrupar_por": "ano", "nomes_indices": "PRCPTOT"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["agregacao"] == "media"
    assert body["agrupar_por"] == ["ano"]
    mapa = {g["grupo"]["ano"]: g for g in body["grupos"]}
    # Ano 2026 PRCPTOT: valores [1200, 1000, 1300] -> 1166.67
    assert mapa[2026]["valor"] == pytest.approx((1200 + 1000 + 1300) / 3, rel=1e-6)
    assert mapa[2027]["valor"] == pytest.approx(1250.0)


@pytest.mark.asyncio
async def test_get_agregados_count_global(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados/agregados", params={"agregacao": "count"})
    body = resp.json()
    assert len(body["grupos"]) == 1
    assert body["grupos"][0]["valor"] == 6.0


@pytest.mark.asyncio
async def test_get_agregados_agregacao_invalida_retorna_422(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados/agregados", params={"agregacao": "soma"})
    # Validação do FastAPI (pattern regex no Query) → 422 com detail de validação.
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_agregados_agrupar_por_invalido_retorna_422(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados/agregados", params={"agrupar_por": "xpto"})
    assert resp.status_code == 422
    body = resp.json()
    assert "agrupar_por" in body.get("detail", "").lower()


@pytest.mark.asyncio
async def test_get_agregados_por_cenario_usa_join(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get(
        "/api/resultados/agregados",
        params={"agregacao": "media", "agrupar_por": "cenario", "nomes_indices": "PRCPTOT"},
    )
    body = resp.json()
    mapa = {g["grupo"]["cenario"]: g for g in body["grupos"]}
    assert mapa["rcp45"]["valor"] == pytest.approx((1200 + 1250 + 1000) / 3)
    assert mapa["rcp85"]["valor"] == pytest.approx(1300.0)


@pytest.mark.asyncio
async def test_get_stats(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _popular(async_sessionmaker_)
    resp = await cliente_api.get("/api/resultados/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert sorted(body["cenarios"]) == ["rcp45", "rcp85"]
    assert body["anos"] == [2026, 2027]
    assert body["variaveis"] == ["pr"]
    assert sorted(body["nomes_indices"]) == ["CDD", "PRCPTOT"]
    assert body["total_execucoes_com_resultados"] == 2
    assert body["total_resultados"] == 6


@pytest.mark.asyncio
async def test_get_stats_banco_vazio(cliente_api: AsyncClient) -> None:
    resp = await cliente_api.get("/api/resultados/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_resultados"] == 0
    assert body["cenarios"] == []


@pytest.mark.asyncio
async def test_get_resultados_limit_acima_do_maximo_rejeita(
    cliente_api: AsyncClient,
) -> None:
    resp = await cliente_api.get("/api/resultados", params={"limit": 2000})
    # 422 do Pydantic (regra ``le=1000`` no Query).
    assert resp.status_code == 422
