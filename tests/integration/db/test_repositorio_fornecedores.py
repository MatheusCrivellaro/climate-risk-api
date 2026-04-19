"""Testes de :class:`SQLAlchemyRepositorioFornecedores` (Slice 10)."""

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
    nome: str = "Fornecedor Alpha",
    cidade: str = "São Paulo",
    uf: str = "SP",
    identificador_externo: str | None = "ext-001",
    lat: float | None = None,
    lon: float | None = None,
) -> Fornecedor:
    agora = datetime(2026, 4, 16, 10, 30, tzinfo=UTC)
    return Fornecedor(
        id=id_ or gerar_id("forn"),
        nome=nome,
        cidade=cidade,
        uf=uf,
        criado_em=agora,
        atualizado_em=agora,
        identificador_externo=identificador_externo,
        lat=lat,
        lon=lon,
        municipio_id=None,
    )


@pytest.mark.asyncio
async def test_criar_buscar_remover(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    fornecedor = _fazer_fornecedor()
    await repo.salvar(fornecedor)

    lido = await repo.buscar_por_id(fornecedor.id)
    assert lido is not None
    assert lido.id == fornecedor.id
    assert lido.cidade == "São Paulo"
    assert lido.uf == "SP"
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
        await repo.salvar(_fazer_fornecedor(id_=fid, nome=f"Forn-{fid[-4:]}"))

    pagina1 = await repo.listar(limit=2, offset=0)
    pagina2 = await repo.listar(limit=2, offset=2)
    pagina3 = await repo.listar(limit=2, offset=4)

    assert len(pagina1) == 2
    assert len(pagina2) == 2
    assert len(pagina3) == 1

    vistos = {f.id for f in (*pagina1, *pagina2, *pagina3)}
    assert vistos == set(ids)
    assert await repo.contar() == 5


@pytest.mark.asyncio
async def test_listar_filtra_por_uf_e_cidade(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    await repo.salvar(_fazer_fornecedor(nome="SP1", cidade="São Paulo", uf="SP"))
    await repo.salvar(_fazer_fornecedor(nome="SP2", cidade="Campinas", uf="SP"))
    await repo.salvar(_fazer_fornecedor(nome="RJ1", cidade="Rio de Janeiro", uf="RJ"))

    assert await repo.contar(uf="SP") == 2
    assert await repo.contar(uf="SP", cidade="Campinas") == 1
    assert await repo.contar(uf="RJ") == 1

    itens_sp = await repo.listar(uf="SP")
    assert {f.nome for f in itens_sp} == {"SP1", "SP2"}


@pytest.mark.asyncio
async def test_buscar_por_nome_cidade_uf(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    f = _fazer_fornecedor(nome="Acme", cidade="São Paulo", uf="SP")
    await repo.salvar(f)

    achado = await repo.buscar_por_nome_cidade_uf("Acme", "São Paulo", "SP")
    assert achado is not None
    assert achado.id == f.id

    assert await repo.buscar_por_nome_cidade_uf("Acme", "Outra", "SP") is None


@pytest.mark.asyncio
async def test_salvar_lote_persiste_todos(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    lote = [_fazer_fornecedor(nome=f"F{i}", cidade="Rio de Janeiro", uf="RJ") for i in range(3)]
    await repo.salvar_lote(lote)

    assert await repo.contar(uf="RJ") == 3


@pytest.mark.asyncio
async def test_salvar_lote_vazio_noop(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioFornecedores(async_session)
    await repo.salvar_lote([])
    assert await repo.contar() == 0
