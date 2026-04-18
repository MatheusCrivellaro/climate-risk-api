"""Testes unitários de :class:`ProcessarCenarioCordex` com fakes in-memory.

Cobre:

- Fluxo completo com bbox restringindo células processadas.
- Persistência em lotes (batch size padrão vs dados pequenos).
- Transições ``pending → running → completed``.
- Transição ``pending → running → failed`` quando leitor falha.
- Iteração outer=iy/inner=ix: células fora do bbox são puladas.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
import pytest

from climate_risk.application.execucoes.processar_cenario import (
    UNIDADES_POR_INDICE,
    ParametrosProcessamento,
    ProcessarCenarioCordex,
)
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada, ErroLeituraNetCDF
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline


@dataclass
class _LeitorFake:
    dados: DadosClimaticos | None = None
    erro: Exception | None = None

    async def abrir(self, caminho: str, variavel: str) -> DadosClimaticos:
        if self.erro is not None:
            raise self.erro
        assert self.dados is not None
        return self.dados


@dataclass
class _RepoExecucoesFake:
    itens: dict[str, Execucao]
    historico: list[tuple[str, str]] = field(default_factory=list)

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        return self.itens.get(execucao_id)

    async def salvar(self, execucao: Execucao) -> None:
        self.itens[execucao.id] = execucao
        self.historico.append((execucao.id, execucao.status))

    async def listar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Execucao]:
        return list(self.itens.values())

    async def contar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
    ) -> int:
        return len(self.itens)


@dataclass
class _RepoResultadosFake:
    lotes: list[list[ResultadoIndice]] = field(default_factory=list)

    async def salvar_lote(self, resultados: Sequence[ResultadoIndice]) -> None:
        self.lotes.append(list(resultados))

    async def listar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        return [r for lote in self.lotes for r in lote]

    async def contar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
    ) -> int:
        return sum(len(lote) for lote in self.lotes)


def _gerar_dados(ny: int = 3, nx: int = 3, anos: tuple[int, int] = (2026, 2027)) -> DadosClimaticos:
    dias_por_ano = 10
    rng = np.random.default_rng(42)
    arrs = []
    anos_arr = []
    for a in anos:
        arrs.append(rng.uniform(0.0, 30.0, size=(dias_por_ano, ny, nx)).astype(np.float32))
        anos_arr.append(np.full(dias_por_ano, a, dtype=np.int64))
    dados = np.concatenate(arrs, axis=0)
    anos_flat = np.concatenate(anos_arr)
    lat_vec = np.linspace(-10.0, 0.0, ny, dtype=np.float64)
    lon_vec = np.linspace(-50.0, -40.0, nx, dtype=np.float64)
    lon_2d, lat_2d = np.meshgrid(lon_vec, lat_vec)
    return DadosClimaticos(
        dados_diarios=dados,
        lat_2d=lat_2d,
        lon_2d=lon_2d,
        anos=anos_flat,
        cenario="rcp45",
        variavel="pr",
        unidade_original="mm/day",
        conversao_unidade_aplicada=False,
        calendario="standard",
        arquivo_origem="/fake/pr.nc",
    )


def _execucao_pending(id_: str = "exec_1") -> Execucao:
    return Execucao(
        id=id_,
        cenario="rcp45",
        variavel="pr",
        arquivo_origem="/fake/pr.nc",
        tipo="grade_bbox",
        parametros={},
        status=StatusExecucao.PENDING,
        criado_em=utc_now(),
        concluido_em=None,
        job_id="job_1",
    )


def _params(
    execucao_id: str = "exec_1", bbox: BoundingBox | None = None
) -> ParametrosProcessamento:
    return ParametrosProcessamento(
        execucao_id=execucao_id,
        arquivo_nc="/fake/pr.nc",
        variavel="pr",
        bbox=bbox,
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2027),
        p95_wet_thr=1.0,
    )


@pytest.mark.asyncio
async def test_processamento_completo_sem_bbox_persiste_todas_celulas() -> None:
    dados = _gerar_dados(ny=3, nx=3, anos=(2026, 2027))
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    sumario = await caso.executar(_params())

    # 3x3 celulas x 2 anos x 8 indices = 144 linhas; tudo em 1 lote (< 1000).
    assert sumario.total_celulas == 9
    assert sumario.total_anos == 2
    assert sumario.total_resultados == 9 * 2 * 8

    todas = [r for lote in repo_res.lotes for r in lote]
    assert len(todas) == sumario.total_resultados
    # Cada célula contribui com exatos 8 nomes de índice por ano.
    assert {r.nome_indice for r in todas} == set(UNIDADES_POR_INDICE)
    # Unidades e execucao_id corretos.
    for r in todas:
        assert r.execucao_id == "exec_1"
        assert r.unidade == UNIDADES_POR_INDICE[r.nome_indice]
        assert r.lat_input is None and r.lon_input is None

    # Transições: pending(inicial já presente) → running → completed.
    estados = [s for (_id, s) in repo_exec.historico]
    assert estados == [StatusExecucao.RUNNING, StatusExecucao.COMPLETED]


@pytest.mark.asyncio
async def test_bbox_restringe_celulas_processadas() -> None:
    dados = _gerar_dados(ny=3, nx=3)
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    # bbox pega apenas a célula central (aprox. lat=-5, lon=-45).
    bbox = BoundingBox(lat_min=-6.0, lat_max=-4.0, lon_min=-46.0, lon_max=-44.0)
    sumario = await caso.executar(_params(bbox=bbox))

    assert sumario.total_celulas == 1
    assert sumario.total_resultados == 1 * 2 * 8


@pytest.mark.asyncio
async def test_bbox_vazia_nao_persiste_nada() -> None:
    dados = _gerar_dados(ny=3, nx=3)
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    bbox = BoundingBox(lat_min=80.0, lat_max=85.0, lon_min=0.0, lon_max=10.0)
    sumario = await caso.executar(_params(bbox=bbox))

    assert sumario.total_celulas == 0
    assert sumario.total_resultados == 0
    assert repo_res.lotes == []
    assert repo_exec.itens["exec_1"].status == StatusExecucao.COMPLETED


@pytest.mark.asyncio
async def test_leitor_falha_marca_execucao_failed_e_propaga() -> None:
    leitor = _LeitorFake(erro=ErroLeituraNetCDF(caminho="/fake/pr.nc", detalhe="corrompido"))
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    with pytest.raises(ErroLeituraNetCDF):
        await caso.executar(_params())

    estados = [s for (_id, s) in repo_exec.historico]
    assert estados == [StatusExecucao.RUNNING, StatusExecucao.FAILED]
    assert repo_exec.itens["exec_1"].concluido_em is not None


@pytest.mark.asyncio
async def test_execucao_inexistente_levanta() -> None:
    dados = _gerar_dados()
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={})
    repo_res = _RepoResultadosFake()
    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.executar(_params("exec_inexistente"))
