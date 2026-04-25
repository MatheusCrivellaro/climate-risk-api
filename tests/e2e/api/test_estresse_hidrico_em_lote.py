"""Testes e2e do endpoint ``/api/execucoes/estresse-hidrico/em-lote`` (Slice 17)."""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.modelos import ExecucaoORM, JobORM


def _criar_pastas(base: Path, *cenarios: str) -> dict[str, dict[str, Path]]:
    pastas: dict[str, dict[str, Path]] = {}
    for cenario in cenarios:
        pastas[cenario] = {}
        for var in ("pr", "tas", "evap"):
            pasta = base / cenario / var
            pasta.mkdir(parents=True, exist_ok=True)
            pastas[cenario][var] = pasta
    return pastas


def _corpo_em_lote(
    pastas: dict[str, dict[str, Path]],
    *,
    pasta_pr_rcp85: Path | None = None,
) -> dict[str, object]:
    rcp85_dir = pastas["rcp85"]
    return {
        "rcp45": {
            "pasta_pr": str(pastas["rcp45"]["pr"]),
            "pasta_tas": str(pastas["rcp45"]["tas"]),
            "pasta_evap": str(pastas["rcp45"]["evap"]),
        },
        "rcp85": {
            "pasta_pr": str(pasta_pr_rcp85 if pasta_pr_rcp85 else rcp85_dir["pr"]),
            "pasta_tas": str(rcp85_dir["tas"]),
            "pasta_evap": str(rcp85_dir["evap"]),
        },
    }


@pytest.mark.asyncio
async def test_em_lote_cria_duas_execucoes_pending(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    pastas = _criar_pastas(tmp_path, "rcp45", "rcp85")

    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico/em-lote",
        json=_corpo_em_lote(pastas),
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    execucoes = corpo["execucoes"]
    assert len(execucoes) == 2
    cenarios = [item["cenario"] for item in execucoes]
    assert cenarios == ["rcp45", "rcp85"]

    for item in execucoes:
        assert item["execucao_id"].startswith("exec_")
        assert item["job_id"].startswith("job_")
        assert item["status"] == StatusExecucao.PENDING
        assert item.get("erro") is None

    async with async_sessionmaker_() as sessao:
        for item in execucoes:
            execucao = (
                await sessao.execute(
                    select(ExecucaoORM).where(ExecucaoORM.id == item["execucao_id"])
                )
            ).scalar_one()
            assert execucao.tipo == "estresse_hidrico"
            assert execucao.cenario == item["cenario"]
            assert execucao.job_id == item["job_id"]

            job = (
                await sessao.execute(select(JobORM).where(JobORM.id == item["job_id"]))
            ).scalar_one()
            assert job.tipo == "processar_estresse_hidrico_pasta"
            assert job.status == StatusJob.PENDING


@pytest.mark.asyncio
async def test_em_lote_falha_em_um_cenario_nao_impede_o_outro(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    pastas = _criar_pastas(tmp_path, "rcp45", "rcp85")

    # rcp85 com pasta_pr inexistente — só esse deve falhar.
    pasta_inexistente = tmp_path / "rcp85" / "pr-inexistente"

    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico/em-lote",
        json=_corpo_em_lote(pastas, pasta_pr_rcp85=pasta_inexistente),
    )
    assert resposta.status_code == 202, resposta.text
    corpo = resposta.json()
    execucoes = {item["cenario"]: item for item in corpo["execucoes"]}

    item_rcp45 = execucoes["rcp45"]
    assert item_rcp45["execucao_id"].startswith("exec_")
    assert item_rcp45["status"] == StatusExecucao.PENDING
    assert item_rcp45.get("erro") is None

    item_rcp85 = execucoes["rcp85"]
    assert item_rcp85.get("execucao_id") is None
    assert item_rcp85.get("erro")
    assert "pasta" in item_rcp85["erro"].lower() or "não" in item_rcp85["erro"]

    async with async_sessionmaker_() as sessao:
        # rcp45 persistida, rcp85 não.
        rcp45_orm = (
            await sessao.execute(
                select(ExecucaoORM).where(ExecucaoORM.id == item_rcp45["execucao_id"])
            )
        ).scalar_one()
        assert rcp45_orm.cenario == "rcp45"

        outras = (
            (await sessao.execute(select(ExecucaoORM).where(ExecucaoORM.cenario == "rcp85")))
            .scalars()
            .all()
        )
        assert outras == []


@pytest.mark.asyncio
async def test_em_lote_corpo_invalido_retorna_422(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.post(
        "/api/execucoes/estresse-hidrico/em-lote",
        json={"rcp45": {"pasta_pr": ""}},
    )
    assert resposta.status_code == 422
