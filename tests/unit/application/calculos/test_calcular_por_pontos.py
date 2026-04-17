"""Testes unitários do caso de uso :class:`CalcularIndicesPorPontos`.

Usa fakes in-memory para :class:`LeitorNetCDF`, :class:`RepositorioExecucoes`
e :class:`RepositorioResultados` — não toca banco nem arquivos reais.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pytest

from climate_risk.application.calculos.calcular_por_pontos import (
    UNIDADES_POR_INDICE,
    CalcularIndicesPorPontos,
    ParametrosCalculo,
    PontoEntradaDominio,
)
from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline


class _LeitorFake:
    """Implementa :class:`LeitorNetCDF` em memória."""

    def __init__(self, dados: DadosClimaticos) -> None:
        self._dados = dados
        self.chamadas: list[tuple[str, str]] = []

    async def abrir(self, caminho: str, variavel: str) -> DadosClimaticos:
        self.chamadas.append((caminho, variavel))
        return self._dados


@dataclass
class _RepoExecucoesFake:
    execucoes: list[Execucao]

    async def salvar(self, execucao: Execucao) -> None:
        self.execucoes.append(execucao)


@dataclass
class _RepoResultadosFake:
    resultados: list[ResultadoIndice]

    async def salvar_lote(self, resultados: Sequence[ResultadoIndice]) -> None:
        self.resultados.extend(resultados)


def _gerar_dados_basicos(ny: int = 4, nx: int = 4) -> DadosClimaticos:
    """Gera :class:`DadosClimaticos` sintético com 2 anos de dados."""
    dias_por_ano = 365
    rng = np.random.default_rng(seed=7)
    dados_2026 = rng.uniform(0.0, 30.0, size=(dias_por_ano, ny, nx)).astype(np.float32)
    dados_2027 = rng.uniform(0.0, 30.0, size=(dias_por_ano, ny, nx)).astype(np.float32)
    dados = np.concatenate([dados_2026, dados_2027], axis=0)
    anos = np.concatenate(
        [
            np.full(dias_por_ano, 2026, dtype=np.int64),
            np.full(dias_por_ano, 2027, dtype=np.int64),
        ]
    )

    # Grade regular simples em torno de -23/-46.
    lat_vec = np.linspace(-24.0, -22.5, ny, dtype=np.float64)
    lon_vec = np.linspace(-47.0, -45.5, nx, dtype=np.float64)
    lon_2d, lat_2d = np.meshgrid(lon_vec, lat_vec)

    return DadosClimaticos(
        dados_diarios=dados,
        lat_2d=lat_2d,
        lon_2d=lon_2d,
        anos=anos,
        cenario="rcp45",
        variavel="pr",
        unidade_original="mm/day",
        conversao_unidade_aplicada=False,
        calendario="standard",
        arquivo_origem="/fake/pr.nc",
    )


_BASELINE_PADRAO = PeriodoBaseline(2026, 2027)


def _parametros(
    *,
    pontos: list[PontoEntradaDominio],
    p95_baseline: PeriodoBaseline | None = _BASELINE_PADRAO,
    persistir: bool = False,
) -> ParametrosCalculo:
    return ParametrosCalculo(
        arquivo_nc="/fake/pr.nc",
        cenario="rcp45",
        variavel="pr",
        pontos=pontos,
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=p95_baseline,
        p95_wet_thr=1.0,
        persistir=persistir,
    )


@pytest.mark.asyncio
async def test_executar_dois_pontos_tres_anos_ignora_ano_ausente() -> None:
    # A fixture tem só 2 anos; pedimos 2 pontos: resultados = 2 * 2 = 4.
    dados = _gerar_dados_basicos()
    leitor = _LeitorFake(dados)
    repo_exec = _RepoExecucoesFake(execucoes=[])
    repo_res = _RepoResultadosFake(resultados=[])
    caso = CalcularIndicesPorPontos(leitor, repo_exec, repo_res)  # type: ignore[arg-type]

    pontos = [
        PontoEntradaDominio(lat=-23.0, lon=-46.0, identificador="A"),
        PontoEntradaDominio(lat=-23.5, lon=-46.5, identificador="B"),
    ]

    resultado = await caso.executar(_parametros(pontos=pontos))

    assert resultado.cenario == "rcp45"
    assert resultado.variavel == "pr"
    assert resultado.execucao_id is None
    assert len(resultado.resultados) == 4
    anos = sorted({r.ano for r in resultado.resultados})
    assert anos == [2026, 2027]
    identificadores = sorted({r.identificador for r in resultado.resultados if r.identificador})
    assert identificadores == ["A", "B"]


@pytest.mark.asyncio
async def test_persistir_grava_execucao_e_resultados_por_indice() -> None:
    dados = _gerar_dados_basicos()
    leitor = _LeitorFake(dados)
    repo_exec = _RepoExecucoesFake(execucoes=[])
    repo_res = _RepoResultadosFake(resultados=[])
    caso = CalcularIndicesPorPontos(leitor, repo_exec, repo_res)  # type: ignore[arg-type]

    pontos = [PontoEntradaDominio(lat=-23.0, lon=-46.0, identificador="A")]
    resultado = await caso.executar(_parametros(pontos=pontos, persistir=True))

    # Um ponto por 2 anos por 8 indices = 16 linhas persistidas.
    assert len(repo_exec.execucoes) == 1
    execucao = repo_exec.execucoes[0]
    assert execucao.status == StatusExecucao.COMPLETED
    assert execucao.tipo == "pontos"
    assert execucao.job_id is None
    assert resultado.execucao_id == execucao.id

    assert len(repo_res.resultados) == 16
    nomes = {r.nome_indice for r in repo_res.resultados}
    assert nomes == set(UNIDADES_POR_INDICE)
    # Todas as linhas devem herdar o execucao_id do passo anterior.
    assert all(r.execucao_id == execucao.id for r in repo_res.resultados)
    # Unidades persistidas conferem com o mapa declarado.
    for r in repo_res.resultados:
        assert r.unidade == UNIDADES_POR_INDICE[r.nome_indice]


@pytest.mark.asyncio
async def test_persistir_false_nao_toca_repositorios() -> None:
    dados = _gerar_dados_basicos()
    leitor = _LeitorFake(dados)
    repo_exec = _RepoExecucoesFake(execucoes=[])
    repo_res = _RepoResultadosFake(resultados=[])
    caso = CalcularIndicesPorPontos(leitor, repo_exec, repo_res)  # type: ignore[arg-type]

    pontos = [PontoEntradaDominio(lat=-23.0, lon=-46.0, identificador="A")]
    resultado = await caso.executar(_parametros(pontos=pontos, persistir=False))

    assert resultado.execucao_id is None
    assert repo_exec.execucoes == []
    assert repo_res.resultados == []


@pytest.mark.asyncio
async def test_p95_baseline_none_forca_r95ptot_nan() -> None:
    dados = _gerar_dados_basicos()
    leitor = _LeitorFake(dados)
    repo_exec = _RepoExecucoesFake(execucoes=[])
    repo_res = _RepoResultadosFake(resultados=[])
    caso = CalcularIndicesPorPontos(leitor, repo_exec, repo_res)  # type: ignore[arg-type]

    pontos = [PontoEntradaDominio(lat=-23.0, lon=-46.0, identificador="A")]
    resultado = await caso.executar(_parametros(pontos=pontos, p95_baseline=None))

    for ponto in resultado.resultados:
        assert np.isnan(ponto.indices.r95ptot_mm)
        assert np.isnan(ponto.indices.r95ptot_frac)
