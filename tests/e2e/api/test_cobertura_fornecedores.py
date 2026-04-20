"""Testes e2e de ``POST /cobertura/fornecedores`` (Slice 9).

Substituímos :class:`ClienteIBGEHttp` por um fake via
``app.dependency_overrides`` — a geocodificação só precisa devolver itens
compatíveis com o esquema do Slice 8, sem tocar rede.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import climate_risk.infrastructure.db.modelos  # noqa: F401
from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.portas.cliente_ibge import MunicipioIBGE
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioMunicipios,
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.interfaces.app import create_app
from climate_risk.interfaces.dependencias import (
    obter_calculador_centroide,
    obter_cliente_ibge,
)


@dataclass
class _ClienteIBGEFake:
    catalogo: list[MunicipioIBGE] = field(default_factory=list)

    async def listar_municipios(self) -> list[MunicipioIBGE]:
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
    def calcular(self, geojson: dict[str, Any]) -> tuple[float, float]:
        return -23.55, -46.63


async def _popular_banco(sessionmaker_: async_sessionmaker[AsyncSession]) -> None:
    """Cria cache IBGE + uma execução com resultado para município 3550308.

    Assim o teste valida todos os ramos da cobertura:
    - São Paulo (3550308): geocodifica E tem resultado → cobertura=True.
    - Rio (3304557): geocodifica mas não tem resultado → ``sem_dados_climaticos``.
    - XYZ/ZZ: não geocodifica → ``municipio_nao_geocodificado``.
    """
    async with sessionmaker_() as sessao:
        repo_m = SQLAlchemyRepositorioMunicipios(sessao)
        await repo_m.salvar_lote(
            [
                Municipio(
                    id=3550308,
                    nome="São Paulo",
                    nome_normalizado="sao paulo",
                    uf="SP",
                    lat_centroide=-23.55,
                    lon_centroide=-46.63,
                    atualizado_em=datetime(2026, 4, 19, tzinfo=UTC),
                ),
                Municipio(
                    id=3304557,
                    nome="Rio de Janeiro",
                    nome_normalizado="rio de janeiro",
                    uf="RJ",
                    lat_centroide=-22.9,
                    lon_centroide=-43.2,
                    atualizado_em=datetime(2026, 4, 19, tzinfo=UTC),
                ),
            ]
        )
        repo_e = SQLAlchemyRepositorioExecucoes(sessao)
        exec_id = gerar_id("exec")
        await repo_e.salvar(
            Execucao(
                id=exec_id,
                cenario="rcp45",
                variavel="pr",
                arquivo_origem="/fake.nc",
                tipo="grade_bbox",
                parametros={},
                status=StatusExecucao.COMPLETED,
                criado_em=datetime(2026, 4, 19, tzinfo=UTC),
                concluido_em=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
                job_id=None,
            )
        )
        repo_r = SQLAlchemyRepositorioResultados(sessao)
        await repo_r.salvar_lote(
            [
                ResultadoIndice(
                    id=gerar_id("res"),
                    execucao_id=exec_id,
                    lat=-23.55,
                    lon=-46.63,
                    lat_input=-23.55,
                    lon_input=-46.63,
                    ano=2026,
                    nome_indice="wet_days",
                    valor=120.0,
                    unidade="dias",
                    municipio_id=3550308,
                ),
            ]
        )


@pytest_asyncio.fixture
async def cliente_cobertura(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    await _popular_banco(async_sessionmaker_)

    app = create_app()

    async def _get_sessao_teste() -> AsyncGenerator[AsyncSession, None]:
        async with async_sessionmaker_() as sessao:
            yield sessao

    cliente_ibge = _ClienteIBGEFake(
        catalogo=[
            MunicipioIBGE(id=3550308, nome="São Paulo", uf="SP"),
            MunicipioIBGE(id=3304557, nome="Rio de Janeiro", uf="RJ"),
        ]
    )
    centroide = _CentroideFixo()

    app.dependency_overrides[get_sessao] = _get_sessao_teste
    app.dependency_overrides[obter_cliente_ibge] = lambda: cliente_ibge
    app.dependency_overrides[obter_calculador_centroide] = lambda: centroide

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as cliente:
        yield cliente


@pytest.mark.asyncio
async def test_cobertura_estruturada(cliente_cobertura: AsyncClient) -> None:
    resposta = await cliente_cobertura.post(
        "/api/cobertura/fornecedores",
        json={
            "fornecedores": [
                {"identificador": "f1", "cidade": "São Paulo", "uf": "SP"},
                {"identificador": "f2", "cidade": "Rio de Janeiro", "uf": "RJ"},
                {"identificador": "f3", "cidade": "Xyzzyx Qwerty", "uf": "ZZ"},
            ]
        },
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total"] == 3
    assert corpo["com_cobertura"] == 1
    assert corpo["sem_cobertura"] == 2
    motivos = {i["identificador"]: i["motivo_nao_encontrado"] for i in corpo["itens"]}
    assert motivos["f1"] is None
    assert motivos["f2"] == "sem_dados_climaticos"
    assert motivos["f3"] == "municipio_nao_geocodificado"


@pytest.mark.asyncio
async def test_cobertura_texto_legacy(cliente_cobertura: AsyncClient) -> None:
    """Formato idêntico ao notebook legacy: CIDADE/UF por linha."""
    resposta = await cliente_cobertura.post(
        "/api/cobertura/fornecedores",
        json={
            "texto_legacy": (
                "São Paulo/SP\n"
                "\n"  # linha em branco ignorada
                "Rio de Janeiro/RJ\n"
                "# comentario ignorado (sem barra)\n"
            )
        },
    )
    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["total"] == 2
    assert corpo["com_cobertura"] == 1
    identificadores = {i["identificador"] for i in corpo["itens"]}
    assert identificadores == {"São Paulo/SP", "Rio de Janeiro/RJ"}


@pytest.mark.asyncio
async def test_cobertura_sem_fornecedores_nem_texto_422(
    cliente_cobertura: AsyncClient,
) -> None:
    resposta = await cliente_cobertura.post("/api/cobertura/fornecedores", json={})
    assert resposta.status_code == 422


@pytest.mark.asyncio
async def test_cobertura_ambos_fornecidos_422(cliente_cobertura: AsyncClient) -> None:
    resposta = await cliente_cobertura.post(
        "/api/cobertura/fornecedores",
        json={
            "fornecedores": [{"identificador": "x", "cidade": "São Paulo", "uf": "SP"}],
            "texto_legacy": "Rio de Janeiro/RJ",
        },
    )
    assert resposta.status_code == 422
