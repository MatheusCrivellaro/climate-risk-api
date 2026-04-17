"""Testes de :class:`SQLAlchemyRepositorioFornecedores`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroConflito
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioFornecedores,
)


def _fazer_fornecedor(
    *,
    id_: str | None = None,
    identificador_externo: str | None = "ext-001",
    nome: str = "Fornecedor Alpha",
    lat: float = -23.55,
    lon: float = -46.63,
) -> Fornecedor:
    return Fornecedor(
        id=id_ or gerar_id("forn"),
        identificador_externo=identificador_externo,
        nome=nome,
        lat=lat,
        lon=lon,
        municipio_id=None,
        criado_em=datetime(2026, 4, 16, 10, 30, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_criar_buscar_remover(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    fornecedor = _fazer_fornecedor()
    await repo.salvar(fornecedor)

    lido = await repo.buscar_por_id(fornecedor.id)
    assert lido is not None
    assert lido.id == fornecedor.id
    assert lido.identificador_externo == "ext-001"

    removido = await repo.remover(fornecedor.id)
    assert removido is True
    assert await repo.buscar_por_id(fornecedor.id) is None
    assert await repo.remover(fornecedor.id) is False


@pytest.mark.asyncio
async def test_salvar_duas_vezes_mesmo_id_levanta_conflito(
    async_session: AsyncSession,
) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    fornecedor = _fazer_fornecedor()
    await repo.salvar(fornecedor)

    with pytest.raises(ErroConflito):
        await repo.salvar(fornecedor)


@pytest.mark.asyncio
async def test_listar_paginada(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    ids = [gerar_id("forn") for _ in range(5)]
    for fid in ids:
        await repo.salvar(_fazer_fornecedor(id_=fid))

    pagina1 = await repo.listar(limit=2, offset=0)
    pagina2 = await repo.listar(limit=2, offset=2)
    pagina3 = await repo.listar(limit=2, offset=4)

    assert len(pagina1) == 2
    assert len(pagina2) == 2
    assert len(pagina3) == 1

    vistos = {f.id for f in (*pagina1, *pagina2, *pagina3)}
    assert vistos == set(ids)
    assert await repo.contar() == 5
