"""Testes e2e de ``POST /localizacoes/geocodificar`` e ``POST /admin/ibge/refresh``.

Substituímos :class:`ClienteIBGEHttp` por um fake via
``app.dependency_overrides`` — isso mantém a rota, DI, schemas, middleware e
repositório reais no circuito, sem tocar a internet.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import climate_risk.infrastructure.db.modelos  # noqa: F401
from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.domain.portas.cliente_ibge import MunicipioIBGE
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.interfaces.app import create_app
from climate_risk.interfaces.dependencias import (
    obter_calculador_centroide,
    obter_cliente_ibge,
)


@dataclass
class _ClienteIBGEFake:
    catalogo: list[MunicipioIBGE] = field(default_factory=list)
    falhar: bool = False

    async def listar_municipios(self) -> list[MunicipioIBGE]:
        if self.falhar:
            raise ErroClienteIBGE("timeout", endpoint="/api/v1/localidades/municipios")
        return list(self.catalogo)

    async def obter_geometria_municipio(self, municipio_id: int) -> dict[str, Any]:
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-46.5, -23.5], [-46.0, -23.5], [-46.0, -23.0], [-46.5, -23.0], [-46.5, -23.5]]
                ],
            },
        }


class _CentroideFixo:
    def __init__(self, lat: float, lon: float) -> None:
        self._lat, self._lon = lat, lon

    def calcular(self, geojson: dict[str, Any]) -> tuple[float, float]:
        return self._lat, self._lon


async def _construir_cliente(
    sessionmaker: async_sessionmaker[AsyncSession],
    cliente_fake: _ClienteIBGEFake,
    centroide: _CentroideFixo,
) -> AsyncGenerator[AsyncClient, None]:
    app = create_app()

    async def _get_sessao_teste() -> AsyncGenerator[AsyncSession, None]:
        async with sessionmaker() as sessao:
            yield sessao

    app.dependency_overrides[get_sessao] = _get_sessao_teste
    app.dependency_overrides[obter_cliente_ibge] = lambda: cliente_fake
    app.dependency_overrides[obter_calculador_centroide] = lambda: centroide

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as cliente:
        yield cliente


@pytest_asyncio.fixture
async def cliente_com_cache(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    fake = _ClienteIBGEFake(
        catalogo=[
            MunicipioIBGE(id=3550308, nome="São Paulo", uf="SP"),
            MunicipioIBGE(id=3509502, nome="Campinas", uf="SP"),
        ],
    )
    async for cliente in _construir_cliente(
        async_sessionmaker_, fake, _CentroideFixo(-23.55, -46.63)
    ):
        yield cliente


@pytest_asyncio.fixture
async def cliente_ibge_indisponivel(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    fake = _ClienteIBGEFake(falhar=True)
    async for cliente in _construir_cliente(async_sessionmaker_, fake, _CentroideFixo(0.0, 0.0)):
        yield cliente


@pytest.mark.asyncio
async def test_geocodificar_cache_populado_pela_primeira_chamada(
    cliente_com_cache: AsyncClient,
) -> None:
    resposta = await cliente_com_cache.post(
        "/api/localizacoes/geocodificar",
        json={
            "localizacoes": [
                {"cidade": "São Paulo", "uf": "SP"},
                {"cidade": "Campinas", "uf": "SP"},
            ]
        },
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total"] == 2
    assert corpo["encontrados"] == 2
    metodos = {item["metodo"] for item in corpo["itens"]}
    # Após o cold start, a segunda entrada já cai em cache_exato.
    assert metodos == {"cache_exato"}


@pytest.mark.asyncio
async def test_geocodificar_ibge_indisponivel_retorna_api_falhou(
    cliente_ibge_indisponivel: AsyncClient,
) -> None:
    resposta = await cliente_ibge_indisponivel.post(
        "/api/localizacoes/geocodificar",
        json={"localizacoes": [{"cidade": "São Paulo", "uf": "SP"}]},
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["itens"][0]["metodo"] == "api_falhou"
    assert corpo["nao_encontrados"] == 1


@pytest.mark.asyncio
async def test_admin_refresh_retorna_sumario(
    cliente_com_cache: AsyncClient,
) -> None:
    resposta = await cliente_com_cache.post("/api/admin/ibge/refresh")
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total_municipios"] == 2
    assert corpo["com_centroide"] == 2
    assert corpo["sem_centroide"] == 0


@pytest.mark.asyncio
async def test_admin_refresh_ibge_indisponivel_retorna_503(
    cliente_ibge_indisponivel: AsyncClient,
) -> None:
    resposta = await cliente_ibge_indisponivel.post("/api/admin/ibge/refresh")
    assert resposta.status_code == 503, resposta.text
    corpo = resposta.json()
    assert corpo["type"].endswith("/ibge-indisponivel")
    assert corpo["status"] == 503


@pytest.mark.asyncio
async def test_geocodificar_entrada_vazia_422(cliente_com_cache: AsyncClient) -> None:
    resposta = await cliente_com_cache.post(
        "/api/localizacoes/geocodificar", json={"localizacoes": []}
    )
    assert resposta.status_code == 422
