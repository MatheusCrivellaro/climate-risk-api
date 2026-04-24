"""Testes e2e dos endpoints de estresse hídrico (Slice 15)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.conversores_tempo import datetime_para_iso
from climate_risk.infrastructure.db.modelos import (
    ExecucaoORM,
    JobORM,
    MunicipioORM,
    ResultadoEstresseHidricoORM,
)


def _corpo_basico(**overrides: object) -> dict[str, object]:
    corpo: dict[str, object] = {
        "arquivo_pr": "",  # preenchido via fixture tmp_path
        "arquivo_tas": "",
        "arquivo_evap": "",
        "cenario": "rcp45",
    }
    corpo.update(overrides)
    return corpo


async def _inserir_municipio(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    id_: int,
    nome: str,
    uf: str,
) -> None:
    agora_iso = datetime_para_iso(datetime.now(UTC))
    async with sessionmaker() as sessao:
        sessao.add(
            MunicipioORM(
                id=id_,
                nome=nome,
                nome_normalizado=nome.lower(),
                uf=uf,
                lat_centroide=None,
                lon_centroide=None,
                atualizado_em=agora_iso,
            )
        )
        await sessao.commit()


async def _inserir_execucao_estresse_hidrico(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    execucao_id: str | None = None,
    cenario: str = "rcp45",
) -> str:
    if execucao_id is None:
        execucao_id = gerar_id("exec")
    agora_iso = datetime_para_iso(datetime.now(UTC))
    async with sessionmaker() as sessao:
        sessao.add(
            ExecucaoORM(
                id=execucao_id,
                cenario=cenario,
                variavel="pr+tas+evap",
                arquivo_origem="/tmp/pr.nc",
                tipo="estresse_hidrico",
                parametros="{}",
                status=StatusExecucao.COMPLETED,
                criado_em=agora_iso,
                concluido_em=agora_iso,
                job_id=None,
            )
        )
        await sessao.commit()
    return execucao_id


async def _inserir_resultado(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    execucao_id: str,
    municipio_id: int,
    ano: int,
    cenario: str,
    frequencia: int = 10,
    intensidade: float = 12.5,
) -> str:
    rid = gerar_id("reh")
    agora_iso = datetime_para_iso(datetime.now(UTC))
    async with sessionmaker() as sessao:
        sessao.add(
            ResultadoEstresseHidricoORM(
                id=rid,
                execucao_id=execucao_id,
                municipio_id=municipio_id,
                ano=ano,
                cenario=cenario,
                frequencia_dias_secos_quentes=frequencia,
                intensidade_mm=intensidade,
                criado_em=agora_iso,
            )
        )
        await sessao.commit()
    return rid


# ---------------------------------------------------------------------
# POST /execucoes/estresse-hidrico
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_criar_execucao_estresse_hidrico_retorna_202(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    pr = tmp_path / "pr.nc"
    tas = tmp_path / "tas.nc"
    evap = tmp_path / "evap.nc"
    for p in (pr, tas, evap):
        p.write_bytes(b"")

    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico",
        json=_corpo_basico(arquivo_pr=str(pr), arquivo_tas=str(tas), arquivo_evap=str(evap)),
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    execucao_id = corpo["execucao_id"]
    job_id = corpo["job_id"]
    assert execucao_id.startswith("exec_")
    assert job_id.startswith("job_")
    assert corpo["status"] == StatusExecucao.PENDING
    assert corpo["links"] == {
        "self": f"/api/execucoes/{execucao_id}",
        "job": f"/api/jobs/{job_id}",
    }

    async with async_sessionmaker_() as sessao:
        execucao = (
            await sessao.execute(select(ExecucaoORM).where(ExecucaoORM.id == execucao_id))
        ).scalar_one()
        assert execucao.tipo == "estresse_hidrico"
        assert execucao.cenario == "rcp45"
        assert execucao.job_id == job_id

        job = (await sessao.execute(select(JobORM).where(JobORM.id == job_id))).scalar_one()
        assert job.tipo == "processar_estresse_hidrico"
        assert job.status == StatusJob.PENDING


@pytest.mark.asyncio
async def test_criar_execucao_estresse_hidrico_arquivo_ausente_retorna_404(
    cliente_api: AsyncClient,
    tmp_path: Path,
) -> None:
    pr = tmp_path / "pr.nc"
    tas = tmp_path / "tas.nc"
    pr.write_bytes(b"")
    tas.write_bytes(b"")
    # evap não criado

    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico",
        json=_corpo_basico(
            arquivo_pr=str(pr),
            arquivo_tas=str(tas),
            arquivo_evap=str(tmp_path / "evap_inexistente.nc"),
        ),
    )
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"].endswith("/arquivo-nc-nao-encontrado")


@pytest.mark.asyncio
async def test_criar_execucao_estresse_hidrico_cenario_invalido_retorna_422(
    cliente_api: AsyncClient,
    tmp_path: Path,
) -> None:
    pr = tmp_path / "pr.nc"
    tas = tmp_path / "tas.nc"
    evap = tmp_path / "evap.nc"
    for p in (pr, tas, evap):
        p.write_bytes(b"")

    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico",
        json=_corpo_basico(
            arquivo_pr=str(pr),
            arquivo_tas=str(tas),
            arquivo_evap=str(evap),
            cenario="cenario-invalido",
        ),
    )
    assert resposta.status_code == 422


@pytest.mark.asyncio
async def test_criar_execucao_estresse_hidrico_path_vazio_retorna_422(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico",
        json=_corpo_basico(arquivo_pr="", arquivo_tas="", arquivo_evap=""),
    )
    assert resposta.status_code == 422


# ---------------------------------------------------------------------
# GET /resultados/estresse-hidrico
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_listar_resultados_estresse_hidrico_sem_dados(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.get("/api/resultados/estresse-hidrico")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total"] == 0
    assert corpo["items"] == []
    assert corpo["limit"] == 100
    assert corpo["offset"] == 0


@pytest.mark.asyncio
async def test_listar_resultados_estresse_hidrico_paginacao_e_filtros(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _inserir_municipio(async_sessionmaker_, id_=3550308, nome="São Paulo", uf="SP")
    await _inserir_municipio(async_sessionmaker_, id_=3304557, nome="Rio de Janeiro", uf="RJ")
    exec_a = await _inserir_execucao_estresse_hidrico(async_sessionmaker_, cenario="rcp45")
    exec_b = await _inserir_execucao_estresse_hidrico(async_sessionmaker_, cenario="rcp85")

    for ano in (2026, 2027, 2028):
        await _inserir_resultado(
            async_sessionmaker_,
            execucao_id=exec_a,
            municipio_id=3550308,
            ano=ano,
            cenario="rcp45",
            frequencia=ano - 2000,
            intensidade=float(ano - 2000) * 1.5,
        )
        await _inserir_resultado(
            async_sessionmaker_,
            execucao_id=exec_a,
            municipio_id=3304557,
            ano=ano,
            cenario="rcp45",
        )
    await _inserir_resultado(
        async_sessionmaker_,
        execucao_id=exec_b,
        municipio_id=3550308,
        ano=2026,
        cenario="rcp85",
    )

    resposta = await cliente_api.get("/api/resultados/estresse-hidrico")
    corpo = resposta.json()
    assert corpo["total"] == 7

    # Filtro por execucao
    r1 = await cliente_api.get(
        "/api/resultados/estresse-hidrico",
        params={"execucao_id": exec_b},
    )
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["total"] == 1
    assert body1["items"][0]["execucao_id"] == exec_b

    # Filtro por cenário
    r2 = await cliente_api.get("/api/resultados/estresse-hidrico", params={"cenario": "rcp45"})
    assert r2.json()["total"] == 6

    # Filtro por ano exato
    r3 = await cliente_api.get("/api/resultados/estresse-hidrico", params={"ano": 2027})
    assert r3.json()["total"] == 2

    # Filtro por range de anos
    r4 = await cliente_api.get(
        "/api/resultados/estresse-hidrico",
        params={"ano_min": 2027, "ano_max": 2028, "cenario": "rcp45"},
    )
    assert r4.json()["total"] == 4

    # Filtro por UF (com JOIN + enrichment)
    r5 = await cliente_api.get("/api/resultados/estresse-hidrico", params={"uf": "SP"})
    body5 = r5.json()
    assert body5["total"] == 4  # 3 do exec_a + 1 do exec_b
    for item in body5["items"]:
        assert item["nome_municipio"] == "São Paulo"
        assert item["uf"] == "SP"

    # Filtro por municipio_id
    r6 = await cliente_api.get(
        "/api/resultados/estresse-hidrico",
        params={"municipio_id": 3304557},
    )
    assert r6.json()["total"] == 3

    # Paginação
    r7 = await cliente_api.get(
        "/api/resultados/estresse-hidrico",
        params={"limit": 2, "offset": 0},
    )
    body7 = r7.json()
    assert body7["total"] == 7
    assert len(body7["items"]) == 2
    assert body7["limit"] == 2
    assert body7["offset"] == 0


@pytest.mark.asyncio
async def test_listar_resultados_estresse_hidrico_uf_minuscula_normaliza(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _inserir_municipio(async_sessionmaker_, id_=3550308, nome="SP", uf="SP")
    exec_id = await _inserir_execucao_estresse_hidrico(async_sessionmaker_)
    await _inserir_resultado(
        async_sessionmaker_,
        execucao_id=exec_id,
        municipio_id=3550308,
        ano=2026,
        cenario="rcp45",
    )
    resposta = await cliente_api.get("/api/resultados/estresse-hidrico", params={"uf": "sp"})
    assert resposta.status_code == 200
    assert resposta.json()["total"] == 1
