"""Testes e2e de ``POST /localizacoes/localizar`` (Slice 9)."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from climate_risk.domain.portas.shapefile_municipios import (
    LocalizacaoGeografica,
    ShapefileMunicipios,
)
from climate_risk.interfaces.app import create_app
from climate_risk.interfaces.dependencias import obter_shapefile


class _ShapefileFake(ShapefileMunicipios):
    def __init__(self, mapa: dict[tuple[float, float], LocalizacaoGeografica]) -> None:
        self._mapa = mapa

    def localizar_ponto(self, lat: float, lon: float) -> LocalizacaoGeografica | None:
        return self._mapa.get((lat, lon))

    def localizar_pontos(
        self, pontos: list[tuple[float, float]]
    ) -> list[LocalizacaoGeografica | None]:
        return [self._mapa.get(p) for p in pontos]


@pytest_asyncio.fixture
async def cliente_com_shapefile() -> AsyncGenerator[AsyncClient, None]:
    fake = _ShapefileFake(
        {
            (-23.55, -46.63): LocalizacaoGeografica(
                municipio_id=3550308, uf="SP", nome_municipio="São Paulo"
            ),
            (-22.90, -43.20): LocalizacaoGeografica(
                municipio_id=3304557, uf="RJ", nome_municipio="Rio de Janeiro"
            ),
        }
    )
    app = create_app()
    app.dependency_overrides[obter_shapefile] = lambda: fake
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as cliente:
        yield cliente


@pytest.mark.asyncio
async def test_localizar_dois_encontrados_um_fora(
    cliente_com_shapefile: AsyncClient,
) -> None:
    resposta = await cliente_com_shapefile.post(
        "/api/localizacoes/localizar",
        json={
            "pontos": [
                {"lat": -23.55, "lon": -46.63, "identificador": "forn-1"},
                {"lat": -22.90, "lon": -43.20},
                {"lat": 0.0, "lon": 0.0},
            ]
        },
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total"] == 3
    assert corpo["encontrados"] == 2
    assert corpo["itens"][0]["municipio_id"] == 3550308
    assert corpo["itens"][0]["identificador"] == "forn-1"
    assert corpo["itens"][0]["encontrado"] is True
    assert corpo["itens"][1]["uf"] == "RJ"
    assert corpo["itens"][2]["encontrado"] is False
    assert corpo["itens"][2]["municipio_id"] is None


@pytest.mark.asyncio
async def test_localizar_lote_vazio_422(cliente_com_shapefile: AsyncClient) -> None:
    resposta = await cliente_com_shapefile.post("/api/localizacoes/localizar", json={"pontos": []})
    assert resposta.status_code == 422


@pytest.mark.asyncio
async def test_localizar_lat_invalida_422(cliente_com_shapefile: AsyncClient) -> None:
    resposta = await cliente_com_shapefile.post(
        "/api/localizacoes/localizar",
        json={"pontos": [{"lat": 100.0, "lon": 0.0}]},
    )
    assert resposta.status_code == 422
