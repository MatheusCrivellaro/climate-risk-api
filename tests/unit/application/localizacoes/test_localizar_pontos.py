"""Testes de :class:`LocalizarPontos` com shapefile mockado."""

from __future__ import annotations

import pytest

from climate_risk.application.localizacoes import LocalizarPontos, PontoParaLocalizar
from climate_risk.domain.portas.shapefile_municipios import LocalizacaoGeografica


class _ShapefileFake:
    """Mock em memória: resolve por dicionário ``(lat, lon)`` exato."""

    def __init__(self, mapa: dict[tuple[float, float], LocalizacaoGeografica]) -> None:
        self._mapa = mapa
        self.ultima_chamada_lote: list[tuple[float, float]] | None = None

    def localizar_ponto(self, lat: float, lon: float) -> LocalizacaoGeografica | None:
        return self._mapa.get((lat, lon))

    def localizar_pontos(
        self, pontos: list[tuple[float, float]]
    ) -> list[LocalizacaoGeografica | None]:
        self.ultima_chamada_lote = pontos
        return [self._mapa.get(p) for p in pontos]


@pytest.mark.asyncio
async def test_dois_encontrados_um_fora() -> None:
    sp = LocalizacaoGeografica(municipio_id=3550308, uf="SP", nome_municipio="São Paulo")
    rj = LocalizacaoGeografica(municipio_id=3304557, uf="RJ", nome_municipio="Rio de Janeiro")
    fake = _ShapefileFake({(-23.55, -46.63): sp, (-22.90, -43.20): rj})
    caso = LocalizarPontos(shapefile=fake)

    pontos = [
        PontoParaLocalizar(lat=-23.55, lon=-46.63, identificador="forn-1"),
        PontoParaLocalizar(lat=-22.90, lon=-43.20, identificador="forn-2"),
        PontoParaLocalizar(lat=0.0, lon=0.0),  # no meio do Atlântico
    ]
    resultado = await caso.executar(pontos)

    assert resultado.total == 3
    assert resultado.encontrados == 2
    assert [i.encontrado for i in resultado.itens] == [True, True, False]
    assert resultado.itens[0].municipio_id == 3550308
    assert resultado.itens[0].identificador == "forn-1"
    assert resultado.itens[2].municipio_id is None
    assert resultado.itens[2].uf is None


@pytest.mark.asyncio
async def test_lote_vazio_nao_chama_shapefile() -> None:
    fake = _ShapefileFake({})
    caso = LocalizarPontos(shapefile=fake)
    resultado = await caso.executar([])
    assert resultado.total == 0
    assert resultado.encontrados == 0
    assert resultado.itens == []
    assert fake.ultima_chamada_lote is None


@pytest.mark.asyncio
async def test_identificador_ausente_eh_echoado_como_none() -> None:
    sp = LocalizacaoGeografica(municipio_id=1, uf="SP", nome_municipio="X")
    fake = _ShapefileFake({(-10.0, -40.0): sp})
    caso = LocalizarPontos(shapefile=fake)
    resultado = await caso.executar([PontoParaLocalizar(lat=-10.0, lon=-40.0)])
    assert resultado.itens[0].identificador is None
    assert resultado.itens[0].encontrado is True


@pytest.mark.asyncio
async def test_chamada_shapefile_e_vetorizada() -> None:
    """Garante que o caso de uso chama ``localizar_pontos`` uma única vez."""
    fake = _ShapefileFake({})
    caso = LocalizarPontos(shapefile=fake)
    await caso.executar(
        [
            PontoParaLocalizar(lat=1.0, lon=2.0),
            PontoParaLocalizar(lat=3.0, lon=4.0),
        ]
    )
    assert fake.ultima_chamada_lote == [(1.0, 2.0), (3.0, 4.0)]
