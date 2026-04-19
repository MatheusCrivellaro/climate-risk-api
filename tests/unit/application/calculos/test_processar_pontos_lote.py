"""Testes unitários de :class:`ProcessarPontosLote` (Slice 7 — lado assíncrono)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
import pytest

from climate_risk.application.calculos.calcular_por_pontos import (
    UNIDADES_POR_INDICE,
    PontoEntradaDominio,
)
from climate_risk.application.calculos.processar_pontos_lote import (
    TAMANHO_LOTE_PERSISTENCIA,
    ParametrosProcessamentoPontos,
    ProcessarPontosLote,
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


def _gerar_dados(ny: int = 3, nx: int = 3, anos: tuple[int, ...] = (2026, 2027)) -> DadosClimaticos:
    dias_por_ano = 10
    rng = np.random.default_rng(42)
    arrs = []
    anos_arr = []
    for a in anos:
        arrs.append(rng.uniform(0.0, 30.0, size=(dias_por_ano, ny, nx)).astype(np.float32))
        anos_arr.append(np.full(dias_por_ano, a, dtype=np.int64))
    dados = np.concatenate(arrs, axis=0)
    anos_flat = np.concatenate(anos_arr)
    lat_vec = np.linspace(-25.0, -20.0, ny, dtype=np.float64)
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
        tipo="pontos_lote",
        parametros={},
        status=StatusExecucao.PENDING,
        criado_em=utc_now(),
        concluido_em=None,
        job_id="job_1",
    )


def _pontos(n: int) -> list[PontoEntradaDominio]:
    return [
        PontoEntradaDominio(lat=-22.9 + 0.01 * i, lon=-46.5, identificador=f"P{i}")
        for i in range(n)
    ]


def _params(
    execucao_id: str = "exec_1",
    pontos: list[PontoEntradaDominio] | None = None,
) -> ParametrosProcessamentoPontos:
    return ParametrosProcessamentoPontos(
        execucao_id=execucao_id,
        arquivo_nc="/fake/pr.nc",
        cenario="rcp45",
        variavel="pr",
        pontos=pontos if pontos is not None else _pontos(3),
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2027),
        p95_wet_thr=1.0,
    )


@pytest.mark.asyncio
async def test_happy_path_flatten_com_execucao_id_e_transicoes() -> None:
    dados = _gerar_dados(ny=3, nx=3, anos=(2026, 2027))
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarPontosLote(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    sumario = await caso.executar(_params(pontos=_pontos(3)))

    # 3 pontos x 2 anos x 8 indices = 48 linhas; cabe em 1 lote (< 1000).
    assert sumario.total_pontos == 3
    assert sumario.total_resultados == 3 * 2 * 8
    assert sumario.execucao_id == "exec_1"

    todas = [r for lote in repo_res.lotes for r in lote]
    assert len(todas) == 48
    # execucao_id carregado da execução atual, não da chamada síncrona.
    for r in todas:
        assert r.execucao_id == "exec_1"
        assert r.unidade == UNIDADES_POR_INDICE[r.nome_indice]
        # Slice 7 preserva lat_input/lon_input (fornecidos pelo usuário).
        assert r.lat_input is not None
        assert r.lon_input is not None

    # Nomes de índice cobrem exatamente os 8 esperados.
    assert {r.nome_indice for r in todas} == set(UNIDADES_POR_INDICE)

    # Transições: pending (inicial) → running → completed.
    estados = [s for (_id, s) in repo_exec.historico]
    assert estados == [StatusExecucao.RUNNING, StatusExecucao.COMPLETED]
    assert repo_exec.itens["exec_1"].concluido_em is not None


@pytest.mark.asyncio
async def test_persiste_em_multiplos_lotes_quando_excede_tamanho() -> None:
    # 65 pontos x 2 anos x 8 indices = 1040 linhas → 2 lotes (1000 + 40).
    dados = _gerar_dados(ny=3, nx=3, anos=(2026, 2027))
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarPontosLote(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    sumario = await caso.executar(_params(pontos=_pontos(65)))

    assert sumario.total_resultados == 65 * 2 * 8
    assert len(repo_res.lotes) == 2
    assert len(repo_res.lotes[0]) == TAMANHO_LOTE_PERSISTENCIA
    assert len(repo_res.lotes[1]) == 65 * 2 * 8 - TAMANHO_LOTE_PERSISTENCIA


@pytest.mark.asyncio
async def test_sem_pontos_nao_gera_resultados_mas_completa() -> None:
    dados = _gerar_dados()
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarPontosLote(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    sumario = await caso.executar(_params(pontos=[]))

    assert sumario.total_resultados == 0
    assert repo_res.lotes == []
    # Mesmo sem pontos, a execução transiciona para completed.
    estados = [s for (_id, s) in repo_exec.historico]
    assert estados == [StatusExecucao.RUNNING, StatusExecucao.COMPLETED]


@pytest.mark.asyncio
async def test_leitor_falha_marca_failed_e_propaga() -> None:
    leitor = _LeitorFake(erro=ErroLeituraNetCDF(caminho="/fake/pr.nc", detalhe="corrompido"))
    repo_exec = _RepoExecucoesFake(itens={"exec_1": _execucao_pending()})
    repo_res = _RepoResultadosFake()
    caso = ProcessarPontosLote(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    with pytest.raises(ErroLeituraNetCDF):
        await caso.executar(_params())

    estados = [s for (_id, s) in repo_exec.historico]
    assert estados == [StatusExecucao.RUNNING, StatusExecucao.FAILED]
    assert repo_exec.itens["exec_1"].concluido_em is not None
    assert repo_res.lotes == []


@pytest.mark.asyncio
async def test_execucao_inexistente_levanta_entidade_nao_encontrada() -> None:
    dados = _gerar_dados()
    leitor = _LeitorFake(dados=dados)
    repo_exec = _RepoExecucoesFake(itens={})
    repo_res = _RepoResultadosFake()
    caso = ProcessarPontosLote(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_exec,
        repositorio_resultados=repo_res,
    )

    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.executar(_params("exec_nao_existe"))

    # Falha antes da transição running — nenhum save foi feito.
    assert repo_exec.historico == []
    assert repo_res.lotes == []
