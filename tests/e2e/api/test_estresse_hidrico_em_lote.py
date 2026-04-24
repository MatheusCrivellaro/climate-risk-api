"""Testes e2e do endpoint ``POST /api/execucoes/estresse-hidrico/em-lote`` (Slice 17)."""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.infrastructure.db.modelos import ExecucaoORM, JobORM


def _criar_pastas(tmp_path: Path) -> dict[str, Path]:
    pastas = {}
    for cenario in ("rcp45", "rcp85"):
        for var in ("pr", "tas", "evap"):
            pasta = tmp_path / cenario / var
            pasta.mkdir(parents=True, exist_ok=True)
            pastas[f"{cenario}_{var}"] = pasta
    return pastas


def _payload_para(pastas: dict[str, Path]) -> dict[str, object]:
    return {
        "rcp45": {
            "pasta_pr": str(pastas["rcp45_pr"]),
            "pasta_tas": str(pastas["rcp45_tas"]),
            "pasta_evap": str(pastas["rcp45_evap"]),
        },
        "rcp85": {
            "pasta_pr": str(pastas["rcp85_pr"]),
            "pasta_tas": str(pastas["rcp85_tas"]),
            "pasta_evap": str(pastas["rcp85_evap"]),
        },
        "parametros": {"limiar_pr_mm_dia": 1.0, "limiar_tas_c": 30.0},
    }


@pytest.mark.asyncio
async def test_em_lote_cria_duas_execucoes_independentes(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    pastas = _criar_pastas(tmp_path)
    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico/em-lote",
        json=_payload_para(pastas),
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    assert len(corpo["execucoes"]) == 2

    cenarios_resposta = {item["cenario"] for item in corpo["execucoes"]}
    assert cenarios_resposta == {"rcp45", "rcp85"}
    for item in corpo["execucoes"]:
        assert item["execucao_id"] is not None
        assert item["execucao_id"].startswith("exec_")
        assert item["job_id"].startswith("job_")
        assert item["status"] == StatusExecucao.PENDING
        assert item["erro"] is None

    async with async_sessionmaker_() as sessao:
        execucoes = (await sessao.execute(select(ExecucaoORM))).scalars().all()
        assert len(execucoes) == 2
        cenarios_db = {e.cenario for e in execucoes}
        assert cenarios_db == {"rcp45", "rcp85"}
        for execucao in execucoes:
            assert execucao.tipo == "estresse_hidrico"
            assert execucao.job_id is not None

        jobs = (await sessao.execute(select(JobORM))).scalars().all()
        assert len(jobs) == 2
        for job in jobs:
            assert job.tipo == "processar_estresse_hidrico_pasta"


@pytest.mark.asyncio
async def test_em_lote_pasta_de_um_cenario_inexistente_nao_afeta_o_outro(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    pastas = _criar_pastas(tmp_path)
    payload = _payload_para(pastas)
    payload["rcp85"]["pasta_pr"] = str(tmp_path / "nao_existe")  # type: ignore[index]

    resposta = await cliente_api.post("/api/execucoes/estresse-hidrico/em-lote", json=payload)
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    assert len(corpo["execucoes"]) == 2
    por_cenario = {item["cenario"]: item for item in corpo["execucoes"]}
    assert por_cenario["rcp45"]["execucao_id"] is not None
    assert por_cenario["rcp45"]["erro"] is None
    assert por_cenario["rcp85"]["execucao_id"] is None
    assert por_cenario["rcp85"]["erro"] is not None

    async with async_sessionmaker_() as sessao:
        execucoes = (await sessao.execute(select(ExecucaoORM))).scalars().all()
        assert len(execucoes) == 1
        assert execucoes[0].cenario == "rcp45"


@pytest.mark.asyncio
async def test_em_lote_payload_invalido_retorna_422(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico/em-lote",
        json={"rcp45": {"pasta_pr": "x"}},  # falta tas/evap, falta rcp85
    )
    assert resposta.status_code == 422


@pytest.mark.asyncio
async def test_em_lote_endpoint_antigo_continua_funcionando(
    cliente_api: AsyncClient,
    tmp_path: Path,
) -> None:
    """Endpoint /api/execucoes/estresse-hidrico (arquivo único) deve permanecer."""
    pr = tmp_path / "pr.nc"
    tas = tmp_path / "tas.nc"
    evap = tmp_path / "evap.nc"
    for p in (pr, tas, evap):
        p.write_bytes(b"")
    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico",
        json={
            "arquivo_pr": str(pr),
            "arquivo_tas": str(tas),
            "arquivo_evap": str(evap),
            "cenario": "rcp45",
        },
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    assert corpo["execucao_id"].startswith("exec_")
