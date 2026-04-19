"""Testes unitários de :class:`GeocodificarLocalizacoes` (Slice 8)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from climate_risk.application.geocodificacao import (
    EntradaLocalizacao,
    GeocodificarLocalizacoes,
    RefreshCatalogoIBGE,
)
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.domain.portas.cliente_ibge import MunicipioIBGE
from climate_risk.domain.util.normalizacao import normalizar_nome_municipio

AGORA = datetime(2026, 4, 19, tzinfo=UTC)


def _municipio(
    id_: int,
    nome: str,
    uf: str,
    lat: float | None = -22.9,
    lon: float | None = -43.2,
) -> Municipio:
    return Municipio(
        id=id_,
        nome=nome,
        nome_normalizado=normalizar_nome_municipio(nome),
        uf=uf,
        lat_centroide=lat,
        lon_centroide=lon,
        atualizado_em=AGORA,
    )


@dataclass
class _RepoMunicipiosFake:
    por_uf: dict[str, list[Municipio]] = field(default_factory=dict)
    lotes_salvos: list[list[Municipio]] = field(default_factory=list)

    async def buscar_por_id(self, municipio_id: int) -> Municipio | None:
        for lista in self.por_uf.values():
            for m in lista:
                if m.id == municipio_id:
                    return m
        return None

    async def buscar_por_nome_uf(self, nome_normalizado: str, uf: str) -> Municipio | None:
        for m in self.por_uf.get(uf, []):
            if m.nome_normalizado == nome_normalizado:
                return m
        return None

    async def listar_por_uf(self, uf: str) -> list[Municipio]:
        return list(self.por_uf.get(uf, []))

    async def salvar(self, municipio: Municipio) -> None:
        self.por_uf.setdefault(municipio.uf, []).append(municipio)

    async def salvar_lote(self, municipios: Sequence[Municipio]) -> None:
        lote = list(municipios)
        self.lotes_salvos.append(lote)
        for m in lote:
            self.por_uf.setdefault(m.uf, []).append(m)

    async def listar(
        self, uf: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[Municipio]:
        if uf is None:
            return [m for lista in self.por_uf.values() for m in lista][offset : offset + limit]
        return self.por_uf.get(uf, [])[offset : offset + limit]

    async def contar(self, uf: str | None = None) -> int:
        if uf is None:
            return sum(len(v) for v in self.por_uf.values())
        return len(self.por_uf.get(uf, []))


@dataclass
class _ClienteIBGEFake:
    catalogo: list[MunicipioIBGE] = field(default_factory=list)
    geometrias: dict[int, dict[str, Any]] = field(default_factory=dict)
    falha_listar: bool = False
    falha_malha_id: int | None = None

    async def listar_municipios(self) -> list[MunicipioIBGE]:
        if self.falha_listar:
            raise ErroClienteIBGE("timeout", endpoint="/api/v1/localidades/municipios")
        return list(self.catalogo)

    async def obter_geometria_municipio(self, municipio_id: int) -> dict[str, Any]:
        if self.falha_malha_id == municipio_id:
            raise ErroClienteIBGE("500", endpoint=f"/malhas/{municipio_id}")
        return self.geometrias.get(municipio_id, _feature_trivial())


def _feature_trivial() -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-46.5, -23.5], [-46.0, -23.5], [-46.0, -23.0], [-46.5, -23.0], [-46.5, -23.5]]
            ],
        },
        "properties": {},
    }


class _CentroideFixo:
    def __init__(self, lat: float, lon: float) -> None:
        self._lat, self._lon = lat, lon

    def calcular(self, geojson: dict[str, Any]) -> tuple[float, float]:
        return self._lat, self._lon


@pytest.mark.asyncio
async def test_cache_exato() -> None:
    sp = _municipio(3550308, "São Paulo", "SP", -23.55, -46.63)
    repo = _RepoMunicipiosFake(por_uf={"SP": [sp]})
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=_ClienteIBGEFake(),
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar([EntradaLocalizacao(cidade="São Paulo", uf="SP")])
    assert resultado.encontrados == 1
    item = resultado.itens[0]
    assert item.metodo == "cache_exato"
    assert item.municipio_id == 3550308
    assert item.lat == pytest.approx(-23.55)
    assert item.lon == pytest.approx(-46.63)
    assert item.nome_canonico == "São Paulo"


@pytest.mark.asyncio
async def test_cache_fuzzy_com_erro_de_digitacao() -> None:
    fpolis = _municipio(4205407, "Florianópolis", "SC")
    repo = _RepoMunicipiosFake(por_uf={"SC": [fpolis]})
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=_ClienteIBGEFake(),
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar([EntradaLocalizacao(cidade="Florianopolis", uf="SC")])
    assert resultado.itens[0].metodo == "cache_exato"  # acentos somem na normalização.

    resultado2 = await caso.executar([EntradaLocalizacao(cidade="Floriano", uf="SC")])
    assert resultado2.itens[0].metodo == "cache_fuzzy"
    assert resultado2.itens[0].municipio_id == 4205407


@pytest.mark.asyncio
async def test_nao_encontrado_fica_sem_bloquear_lote() -> None:
    sp = _municipio(3550308, "São Paulo", "SP")
    repo = _RepoMunicipiosFake(por_uf={"SP": [sp]})
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=_ClienteIBGEFake(),
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar(
        [
            EntradaLocalizacao(cidade="São Paulo", uf="SP"),
            EntradaLocalizacao(cidade="Xyzzyx Qwerty", uf="SP"),
        ]
    )
    assert resultado.total == 2
    assert resultado.encontrados == 1
    assert resultado.nao_encontrados == 1
    assert resultado.itens[0].metodo == "cache_exato"
    assert resultado.itens[1].metodo == "nao_encontrado"
    assert resultado.itens[1].lat is None


@pytest.mark.asyncio
async def test_cold_start_chama_ibge_e_popula_cache() -> None:
    """UF ausente no cache → sincroniza via IBGE e resolve as entradas."""
    repo = _RepoMunicipiosFake()
    cliente = _ClienteIBGEFake(
        catalogo=[
            MunicipioIBGE(id=3550308, nome="São Paulo", uf="SP"),
            MunicipioIBGE(id=3304557, nome="Rio de Janeiro", uf="RJ"),
        ],
        geometrias={3550308: _feature_trivial()},
    )
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=cliente,
        calculador_centroide=_CentroideFixo(-23.55, -46.63),
    )
    resultado = await caso.executar([EntradaLocalizacao(cidade="São Paulo", uf="SP")])
    assert resultado.encontrados == 1
    item = resultado.itens[0]
    assert item.metodo == "cache_exato"
    assert item.municipio_id == 3550308
    assert item.lat == pytest.approx(-23.55)
    assert repo.lotes_salvos  # populou o cache.


@pytest.mark.asyncio
async def test_falha_ibge_marca_api_falhou_mas_nao_quebra_lote() -> None:
    repo = _RepoMunicipiosFake()
    cliente = _ClienteIBGEFake(falha_listar=True)
    # Outra UF já está em cache para provar que só falha a UF problemática.
    rj = _municipio(3304557, "Rio de Janeiro", "RJ")
    repo.por_uf["RJ"] = [rj]
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=cliente,
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar(
        [
            EntradaLocalizacao(cidade="São Paulo", uf="SP"),  # vai falhar
            EntradaLocalizacao(cidade="Rio de Janeiro", uf="RJ"),  # cache hit
            EntradaLocalizacao(cidade="Campinas", uf="SP"),  # mesma UF falha
        ]
    )
    assert resultado.total == 3
    metodos = [i.metodo for i in resultado.itens]
    assert metodos == ["api_falhou", "cache_exato", "api_falhou"]


@pytest.mark.asyncio
async def test_uf_normalizada_para_uppercase() -> None:
    sp = _municipio(3550308, "São Paulo", "SP")
    repo = _RepoMunicipiosFake(por_uf={"SP": [sp]})
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=_ClienteIBGEFake(),
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar([EntradaLocalizacao(cidade="São Paulo", uf="sp")])
    assert resultado.itens[0].uf == "SP"
    assert resultado.itens[0].metodo == "cache_exato"


@pytest.mark.asyncio
async def test_entrada_vazia_fica_nao_encontrado() -> None:
    sp = _municipio(3550308, "São Paulo", "SP")
    repo = _RepoMunicipiosFake(por_uf={"SP": [sp]})
    caso = GeocodificarLocalizacoes(
        repositorio_municipios=repo,
        cliente_ibge=_ClienteIBGEFake(),
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar([EntradaLocalizacao(cidade="   ", uf="SP")])
    assert resultado.itens[0].metodo == "nao_encontrado"


@pytest.mark.asyncio
async def test_refresh_grava_todos_e_conta_centroides() -> None:
    repo = _RepoMunicipiosFake()
    cliente = _ClienteIBGEFake(
        catalogo=[
            MunicipioIBGE(id=3550308, nome="São Paulo", uf="SP"),
            MunicipioIBGE(id=3304557, nome="Rio de Janeiro", uf="RJ"),
        ],
        falha_malha_id=3304557,
    )
    caso = RefreshCatalogoIBGE(
        repositorio_municipios=repo,
        cliente_ibge=cliente,
        calculador_centroide=_CentroideFixo(-23.55, -46.63),
    )
    resultado = await caso.executar()
    assert resultado.total_municipios == 2
    assert resultado.com_centroide == 1
    assert resultado.sem_centroide == 1
    assert len(repo.lotes_salvos) == 1


@pytest.mark.asyncio
async def test_refresh_catalogo_vazio() -> None:
    repo = _RepoMunicipiosFake()
    cliente = _ClienteIBGEFake(catalogo=[])
    caso = RefreshCatalogoIBGE(
        repositorio_municipios=repo,
        cliente_ibge=cliente,
        calculador_centroide=_CentroideFixo(0.0, 0.0),
    )
    resultado = await caso.executar()
    assert resultado.total_municipios == 0
    assert repo.lotes_salvos == []
