"""Testes e2e de ``GET /health/ready`` (Slice 12).

O fixture ``cliente_api`` cria o schema via ``Base.metadata.create_all`` —
ele NÃO roda as migrações Alembic, então ``alembic_version`` só existe se
o próprio teste a criar. Os casos cobertos:

- Sem tabela ``alembic_version`` → ``503`` com motivo ``migrações pendentes``.
- Com revisão divergente → ``503`` com ``revisao_atual`` reportada.
- Com revisão igual ao ``head`` → ``200`` + ``{"status": "ready"}``.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.interfaces.rotas.health import _carregar_head_alembic


async def _definir_revisao(
    sessionmaker: async_sessionmaker[AsyncSession], revisao: str | None
) -> None:
    async with sessionmaker() as sessao:
        await sessao.execute(
            text(
                "CREATE TABLE IF NOT EXISTS alembic_version ("
                "version_num VARCHAR(32) PRIMARY KEY NOT NULL)"
            )
        )
        await sessao.execute(text("DELETE FROM alembic_version"))
        if revisao is not None:
            await sessao.execute(
                text("INSERT INTO alembic_version(version_num) VALUES (:v)"),
                {"v": revisao},
            )
        await sessao.commit()


@pytest.mark.asyncio
async def test_ready_retorna_503_quando_sem_alembic_version(
    cliente_api: AsyncClient,
) -> None:
    resposta = await cliente_api.get("/api/health/ready")

    assert resposta.status_code == 503
    corpo = resposta.json()
    detail = corpo["detail"]
    assert detail["status"] == "unavailable"
    assert detail["motivo"] == "migrações pendentes"
    assert detail["revisao_atual"] is None


@pytest.mark.asyncio
async def test_ready_retorna_503_com_revisao_divergente(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    await _definir_revisao(async_sessionmaker_, "revisao_invalida_xyz")

    resposta = await cliente_api.get("/api/health/ready")

    assert resposta.status_code == 503
    detail = resposta.json()["detail"]
    assert detail["motivo"] == "migrações pendentes"
    assert detail["revisao_atual"] == "revisao_invalida_xyz"
    assert detail["revisao_esperada"] == _carregar_head_alembic()


@pytest.mark.asyncio
async def test_ready_retorna_200_quando_revisao_bate_com_head(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    head = _carregar_head_alembic()
    assert head is not None
    await _definir_revisao(async_sessionmaker_, head)

    resposta = await cliente_api.get("/api/health/ready")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo == {"status": "ready", "revisao_alembic": head}
