"""Testes de :class:`SQLAlchemyRepositorioMunicipios`."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.infrastructure.db.repositorios import SQLAlchemyRepositorioMunicipios


def _fazer_municipio(
    *,
    id_: int = 3550308,
    nome: str = "São Paulo",
    nome_normalizado: str = "sao paulo",
    uf: str = "SP",
    lat: float = -23.55,
    lon: float = -46.63,
) -> Municipio:
    return Municipio(
        id=id_,
        nome=nome,
        nome_normalizado=nome_normalizado,
        uf=uf,
        lat_centroide=lat,
        lon_centroide=lon,
    )


@pytest.mark.asyncio
async def test_salvar_e_buscar_por_id(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    municipio = _fazer_municipio()
    await repo.salvar(municipio)

    resultado = await repo.buscar_por_id(municipio.id)
    assert resultado == municipio


@pytest.mark.asyncio
async def test_buscar_por_nome_uf(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    await repo.salvar(_fazer_municipio())

    resultado = await repo.buscar_por_nome_uf("sao paulo", "SP")
    assert resultado is not None
    assert resultado.nome == "São Paulo"


@pytest.mark.asyncio
async def test_buscar_por_id_inexistente(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    assert await repo.buscar_por_id(9999999) is None


@pytest.mark.asyncio
async def test_upsert_atualiza_campos(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    await repo.salvar(_fazer_municipio(nome="São Paulo"))
    await repo.salvar(_fazer_municipio(nome="Sao Paulo (atualizado)"))

    resultado = await repo.buscar_por_id(3550308)
    assert resultado is not None
    assert resultado.nome == "Sao Paulo (atualizado)"
    assert await repo.contar() == 1


@pytest.mark.asyncio
async def test_listar_com_filtro_uf(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    await repo.salvar(_fazer_municipio(id_=3550308, nome_normalizado="sao paulo", uf="SP"))
    await repo.salvar(_fazer_municipio(id_=3304557, nome_normalizado="rio de janeiro", uf="RJ"))
    await repo.salvar(
        _fazer_municipio(id_=3509502, nome_normalizado="campinas", uf="SP"),
    )

    somente_sp = await repo.listar(uf="SP")
    assert {m.id for m in somente_sp} == {3550308, 3509502}

    todos = await repo.listar()
    assert len(todos) == 3


@pytest.mark.asyncio
async def test_contar(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioMunicipios(async_session)
    await repo.salvar(_fazer_municipio(id_=3550308, uf="SP"))
    await repo.salvar(_fazer_municipio(id_=3304557, uf="RJ"))

    assert await repo.contar() == 2
    assert await repo.contar(uf="SP") == 1
    assert await repo.contar(uf="MG") == 0
