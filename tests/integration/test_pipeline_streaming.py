"""Teste de integração do pipeline streaming de estresse hídrico (Slice 21).

Escopo: handler completo + agregador real (com shapefile sintético) +
SQLite real. Verifica:

- Resultados finais batem com cálculo direto.
- Idempotência: rodar 2x a mesma execução não viola UniqueConstraint.
- Memória durante o pipeline fica abaixo de 200 MB (tracemalloc).

Não cobre o leitor real de NetCDF — fornecemos ``DadosClimaticosMultiVariaveis``
diretamente, mantendo o teste rápido.
"""

from __future__ import annotations

import tracemalloc
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import xarray as xr
from shapely.geometry import box
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import climate_risk.infrastructure.db.modelos  # noqa: F401
from climate_risk.application.jobs.handlers_estresse_hidrico import (
    criar_handler_estresse_hidrico,
)
from climate_risk.core.ids import gerar_id
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
    calcular_indices_anuais_estresse_hidrico,
)
from climate_risk.domain.entidades.dados_multivariaveis import (
    DadosClimaticosMultiVariaveis,
)
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)
from climate_risk.infrastructure.db.base import Base
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioResultadoEstresseHidrico,
)


@pytest_asyncio.fixture
async def async_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = criar_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def async_sessionmaker_(
    async_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return criar_sessionmaker(async_engine)


@pytest_asyncio.fixture
async def async_session(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    async with async_sessionmaker_() as sessao:
        yield sessao


def _gerar_shapefile_sintetico(destino: Path, n: int) -> Path:
    """Cria ``n`` retângulos contíguos em [-50, -30]° lon x [0, 10]° lat."""
    largura = 20.0 / n
    polígonos = [box(-50.0 + i * largura, 0.0, -50.0 + (i + 1) * largura, 10.0) for i in range(n)]
    ids = [str(1000000 + i) for i in range(n)]
    gdf = gpd.GeoDataFrame({"CD_MUN": ids}, geometry=polígonos, crs="EPSG:4326")
    shp = destino / "mun_pipeline.shp"
    gdf.to_file(shp, driver="ESRI Shapefile")
    return shp


def _construir_dados(n_municipios: int) -> DadosClimaticosMultiVariaveis:
    """Grade 1° x ``n`` colunas, 2 anos diários (730 timestamps).

    Valores: pr=0 (sempre seco), tas=35 (sempre quente), evap=5 (déficit
    constante 5.0 mm/dia). Resultado anual analítico.
    """
    tempo = pd.date_range("2030-01-01", periods=730, freq="D")
    lat = np.array([5.0])
    largura = 20.0 / n_municipios
    lon = np.array([-50.0 + (i + 0.5) * largura for i in range(n_municipios)])

    pr_vals = np.zeros((730, 1, n_municipios), dtype=np.float64)
    tas_vals = np.full((730, 1, n_municipios), 35.0, dtype=np.float64)
    evap_vals = np.full((730, 1, n_municipios), 5.0, dtype=np.float64)

    pr = xr.DataArray(
        pr_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="pr",
    )
    tas = xr.DataArray(
        tas_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="tas",
    )
    evap = xr.DataArray(
        evap_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="evap",
    )
    return DadosClimaticosMultiVariaveis(
        precipitacao_diaria_mm=pr,
        temperatura_diaria_c=tas,
        evaporacao_diaria_mm=evap,
        tempo=pd.DatetimeIndex(tempo),
        cenario="rcp45",
    )


class _LeitorFake:
    def __init__(self, dados: DadosClimaticosMultiVariaveis) -> None:
        self._dados = dados

    def abrir(self, **_: Any) -> DadosClimaticosMultiVariaveis:
        return self._dados


def _payload(execucao_id: str) -> dict[str, Any]:
    return {
        "execucao_id": execucao_id,
        "arquivo_pr": "/tmp/pr.nc",
        "arquivo_tas": "/tmp/tas.nc",
        "arquivo_evap": "/tmp/evap.nc",
        "cenario": "rcp45",
        "limiar_pr_mm_dia": 1.0,
        "limiar_tas_c": 30.0,
    }


async def _semear_execucao(sessao: AsyncSession, execucao_id: str) -> None:
    repo = SQLAlchemyRepositorioExecucoes(sessao)
    await repo.salvar(
        Execucao(
            id=execucao_id,
            cenario="rcp45",
            variavel="pr+tas+evap",
            arquivo_origem="/tmp/pr.nc",
            tipo="estresse_hidrico",
            parametros={},
            status=StatusExecucao.PENDING,
            criado_em=datetime(2030, 1, 1, tzinfo=UTC),
            concluido_em=None,
            job_id=None,
        )
    )
    await sessao.commit()


def _construir_handler_streaming(
    sessao: AsyncSession,
    *,
    shapefile: Path,
    cache_dir: Path,
    dados: DadosClimaticosMultiVariaveis,
) -> Any:
    agregador = AgregadorMunicipiosGeopandas(shapefile, cache_dir)
    repo_exec = SQLAlchemyRepositorioExecucoes(sessao)
    repo_res = SQLAlchemyRepositorioResultadoEstresseHidrico(sessao)
    return criar_handler_estresse_hidrico(
        leitor=_LeitorFake(dados),  # type: ignore[arg-type]
        agregador=agregador,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )


@pytest.mark.asyncio
async def test_pipeline_streaming_persiste_resultados_corretos(
    async_session: AsyncSession,
    tmp_path: Path,
) -> None:
    n = 5
    dados = _construir_dados(n_municipios=n)
    shapefile = _gerar_shapefile_sintetico(tmp_path, n=n)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    execucao_id = gerar_id("exec")
    await _semear_execucao(async_session, execucao_id)
    handler = _construir_handler_streaming(
        async_session, shapefile=shapefile, cache_dir=cache_dir, dados=dados
    )

    await handler(_payload(execucao_id))

    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)
    total = await repo.contar(execucao_id=execucao_id)
    assert total == n * 2

    linhas = await repo.listar(execucao_id=execucao_id, limit=100)
    params = ParametrosIndicesEstresseHidrico(limiar_pr_mm_dia=1.0, limiar_tas_c=30.0)
    dias_total = pd.date_range("2030-01-01", periods=730, freq="D")
    for ano in (2030, 2031):
        dias_ano = dias_total[dias_total.year == ano]
        n_dias = len(dias_ano)
        esperado = calcular_indices_anuais_estresse_hidrico(
            pr_mm_dia=np.zeros(n_dias),
            tas_c=np.full(n_dias, 35.0),
            evap_mm_dia=np.full(n_dias, 5.0),
            params=params,
        )
        do_ano = [r for r in linhas if r.ano == ano]
        assert len(do_ano) == n
        for r in do_ano:
            assert r.frequencia_dias_secos_quentes == esperado.dias_secos_quentes
            assert r.intensidade_mm_dia == pytest.approx(esperado.intensidade_mm_dia)


@pytest.mark.asyncio
async def test_pipeline_streaming_idempotente(
    async_session: AsyncSession,
    tmp_path: Path,
) -> None:
    """Rodar 2x a mesma execução produz o mesmo resultado, sem violação de UniqueConstraint."""
    n = 3
    dados = _construir_dados(n_municipios=n)
    shapefile = _gerar_shapefile_sintetico(tmp_path, n=n)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    execucao_id = gerar_id("exec")
    await _semear_execucao(async_session, execucao_id)
    handler = _construir_handler_streaming(
        async_session, shapefile=shapefile, cache_dir=cache_dir, dados=dados
    )

    await handler(_payload(execucao_id))
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)
    total_1 = await repo.contar(execucao_id=execucao_id)
    ids_1 = sorted(r.id for r in await repo.listar(execucao_id=execucao_id, limit=100))
    assert total_1 == n * 2

    # Segunda passada — não deve falhar nem duplicar.
    await handler(_payload(execucao_id))
    total_2 = await repo.contar(execucao_id=execucao_id)
    ids_2 = sorted(r.id for r in await repo.listar(execucao_id=execucao_id, limit=100))

    assert total_2 == n * 2
    # IDs novos foram regerados após o ``deletar_por_execucao``.
    assert set(ids_1).isdisjoint(set(ids_2))


def _construir_dados_com_lon_subset(
    n_municipios: int,
    lon_indices: list[int],
) -> DadosClimaticosMultiVariaveis:
    """Constrói o dataset cobrindo apenas um subconjunto de longitudes/municípios.

    Slice 22: simula uma grade com cobertura municipal reduzida em relação
    ao shapefile (modelo climático com bordas distintas). Demais municípios
    não serão mapeados pela grade.
    """
    tempo = pd.date_range("2030-01-01", periods=730, freq="D")
    lat = np.array([5.0])
    largura = 20.0 / n_municipios
    lon_completo = [-50.0 + (i + 0.5) * largura for i in lon_indices]
    lon = np.array(lon_completo)
    n = len(lon_indices)

    pr_vals = np.zeros((730, 1, n), dtype=np.float64)
    tas_vals = np.full((730, 1, n), 35.0, dtype=np.float64)
    evap_vals = np.full((730, 1, n), 5.0, dtype=np.float64)

    pr = xr.DataArray(
        pr_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="pr",
    )
    tas = xr.DataArray(
        tas_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="tas",
    )
    evap = xr.DataArray(
        evap_vals,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="evap",
    )
    return DadosClimaticosMultiVariaveis(
        precipitacao_diaria_mm=pr,
        temperatura_diaria_c=tas,
        evaporacao_diaria_mm=evap,
        tempo=pd.DatetimeIndex(tempo),
        cenario="rcp45",
    )


@pytest.mark.asyncio
async def test_pipeline_streaming_processa_apenas_interseccao_quando_grades_divergem(
    async_session: AsyncSession,
    tmp_path: Path,
) -> None:
    """Slice 22: pr/tas cobrem A,B,C; evap cobre B,C,D. Pipeline processa só B,C."""
    n = 4  # Municípios A, B, C, D nos índices 0, 1, 2, 3.
    shapefile = _gerar_shapefile_sintetico(tmp_path, n=n)

    # pr e tas: usam grade reduzida — apenas A, B, C (índices 0, 1, 2).
    dados_pr_tas = _construir_dados_com_lon_subset(n_municipios=n, lon_indices=[0, 1, 2])
    # evap: usa grade reduzida — apenas B, C, D (índices 1, 2, 3).
    dados_evap = _construir_dados_com_lon_subset(n_municipios=n, lon_indices=[1, 2, 3])

    dados_combinados = DadosClimaticosMultiVariaveis(
        precipitacao_diaria_mm=dados_pr_tas.precipitacao_diaria_mm,
        temperatura_diaria_c=dados_pr_tas.temperatura_diaria_c,
        evaporacao_diaria_mm=dados_evap.evaporacao_diaria_mm,
        tempo=dados_pr_tas.tempo,
        cenario="rcp45",
    )

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    execucao_id = gerar_id("exec")
    await _semear_execucao(async_session, execucao_id)
    handler = _construir_handler_streaming(
        async_session, shapefile=shapefile, cache_dir=cache_dir, dados=dados_combinados
    )

    await handler(_payload(execucao_id))

    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)
    linhas = await repo.listar(execucao_id=execucao_id, limit=100)
    municipios_persistidos = {r.municipio_id for r in linhas}
    # Apenas a interseção: índices 1, 2 → IDs 1000001, 1000002.
    assert municipios_persistidos == {1000001, 1000002}


@pytest.mark.asyncio
async def test_pipeline_streaming_memoria_limitada(
    async_session: AsyncSession,
    tmp_path: Path,
) -> None:
    """Pipeline com 5 municípios x 2 anos cabe muito abaixo de 200 MB."""
    n = 5
    dados = _construir_dados(n_municipios=n)
    shapefile = _gerar_shapefile_sintetico(tmp_path, n=n)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    execucao_id = gerar_id("exec")
    await _semear_execucao(async_session, execucao_id)
    handler = _construir_handler_streaming(
        async_session, shapefile=shapefile, cache_dir=cache_dir, dados=dados
    )

    tracemalloc.start()
    try:
        await handler(_payload(execucao_id))
        _, pico_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    pico_mb = pico_bytes / (1024 * 1024)
    assert pico_mb < 200, f"Memória de pico excedeu 200 MB: {pico_mb:.1f} MB"
