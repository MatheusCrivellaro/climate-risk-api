"""Testes unitários do handler ``processar_estresse_hidrico`` (Slice 15)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.application.jobs.handlers_estresse_hidrico import (
    criar_handler_estresse_hidrico,
)
from climate_risk.domain.entidades.dados_multivariaveis import DadosClimaticosMultiVariaveis
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)


def _mk_dataarray(valores_por_dia: list[float], tempo: pd.DatetimeIndex) -> xr.DataArray:
    arr = np.array(valores_por_dia, dtype=np.float64)
    return xr.DataArray(arr, dims=["time"], coords={"time": tempo})


@dataclass
class _LeitorFake:
    dados: DadosClimaticosMultiVariaveis

    def abrir(
        self,
        caminho_pr: Path,
        caminho_tas: Path,
        caminho_evap: Path,
    ) -> DadosClimaticosMultiVariaveis:
        return self.dados


@dataclass
class _AgregadorFake:
    """Devolve séries municipais canned para duas cidades."""

    registros_por_variavel: dict[str, pd.DataFrame]

    def agregar_por_municipio(self, dados: xr.DataArray, nome_variavel: str) -> pd.DataFrame:
        return self.registros_por_variavel[nome_variavel]


@dataclass
class _RepoExecucoesFake:
    execucoes: dict[str, Execucao]
    transicoes: list[str] = field(default_factory=list)

    async def salvar(self, execucao: Execucao) -> None:
        self.execucoes[execucao.id] = execucao
        self.transicoes.append(execucao.status)

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        return self.execucoes.get(execucao_id)

    async def listar(self, **_: Any) -> list[Execucao]:
        return list(self.execucoes.values())

    async def contar(self, **_: Any) -> int:
        return len(self.execucoes)


@dataclass
class _RepoResultadosFake:
    salvos: list[ResultadoEstresseHidrico] = field(default_factory=list)

    async def salvar_lote(self, resultados: Any) -> None:
        for r in resultados:
            self.salvos.append(r)

    async def listar(self, **_: Any) -> list[ResultadoEstresseHidrico]:
        return list(self.salvos)

    async def contar(self, **_: Any) -> int:
        return len(self.salvos)


def _fabricar_dados_mult() -> DadosClimaticosMultiVariaveis:
    tempo = pd.date_range("2026-01-01", periods=5, freq="D")
    return DadosClimaticosMultiVariaveis(
        precipitacao_diaria_mm=_mk_dataarray([0.0] * 5, tempo),
        temperatura_diaria_c=_mk_dataarray([32.0] * 5, tempo),
        evaporacao_diaria_mm=_mk_dataarray([3.0] * 5, tempo),
        tempo=pd.DatetimeIndex(tempo),
        cenario="rcp45",
    )


def _fabricar_registros_agregador() -> dict[str, pd.DataFrame]:
    # Município 3550308 (SP capital): 5 dias, todos secos quentes.
    # Município 3304557 (RJ capital): 5 dias, todos chuvosos e frios.
    tempo = pd.date_range("2026-01-01", periods=5, freq="D")

    def _df(mun: str, valores: list[float], nome: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "municipio_id": [mun] * 5,
                "data": list(tempo),
                "valor": valores,
                "nome_variavel": [nome] * 5,
            }
        )

    return {
        "pr": pd.concat(
            [
                _df("3550308", [0.0, 0.0, 0.5, 0.0, 0.2], "pr"),
                _df("3304557", [10.0, 8.0, 5.0, 6.0, 9.0], "pr"),
            ]
        ).reset_index(drop=True),
        "tas": pd.concat(
            [
                _df("3550308", [32.0, 33.0, 31.0, 34.0, 35.0], "tas"),
                _df("3304557", [25.0, 24.0, 22.0, 23.0, 20.0], "tas"),
            ]
        ).reset_index(drop=True),
        "evap": pd.concat(
            [
                _df("3550308", [3.0, 2.5, 2.0, 4.0, 3.5], "evap"),
                _df("3304557", [1.0, 1.2, 0.8, 1.5, 1.0], "evap"),
            ]
        ).reset_index(drop=True),
    }


@pytest.mark.asyncio
async def test_handler_pipeline_completo_persiste_resultados() -> None:
    execucao_id = "exec_teste_01"
    repo_execucoes = _RepoExecucoesFake(
        execucoes={
            execucao_id: Execucao(
                id=execucao_id,
                cenario="rcp45",
                variavel="pr+tas+evap",
                arquivo_origem="/tmp/pr.nc",
                tipo="estresse_hidrico",
                parametros={},
                status=StatusExecucao.PENDING,
                criado_em=datetime(2026, 1, 1, tzinfo=UTC),
                concluido_em=None,
                job_id="job_01",
            )
        }
    )
    repo_resultados = _RepoResultadosFake()
    leitor = _LeitorFake(dados=_fabricar_dados_mult())
    agregador = _AgregadorFake(registros_por_variavel=_fabricar_registros_agregador())

    handler = criar_handler_estresse_hidrico(
        leitor=leitor,  # type: ignore[arg-type]
        agregador=agregador,  # type: ignore[arg-type]
        repositorio_execucoes=repo_execucoes,  # type: ignore[arg-type]
        repositorio_resultados=repo_resultados,  # type: ignore[arg-type]
    )

    payload = {
        "execucao_id": execucao_id,
        "arquivo_pr": "/tmp/pr.nc",
        "arquivo_tas": "/tmp/tas.nc",
        "arquivo_evap": "/tmp/evap.nc",
        "cenario": "rcp45",
        "limiar_pr_mm_dia": 1.0,
        "limiar_tas_c": 30.0,
    }
    await handler(payload)

    # Dois municípios, um ano → 2 linhas persistidas.
    assert len(repo_resultados.salvos) == 2

    # São Paulo (3550308): 5 dias todos secos quentes → frequência 5.
    sp = next(r for r in repo_resultados.salvos if r.municipio_id == 3550308)
    assert sp.ano == 2026
    assert sp.cenario == "rcp45"
    assert sp.frequencia_dias_secos_quentes == 5
    # Intensidade = soma de (evap - pr) nos 5 dias:
    # (3.0-0.0)+(2.5-0.0)+(2.0-0.5)+(4.0-0.0)+(3.5-0.2) = 14.3
    assert sp.intensidade_mm == pytest.approx(14.3)

    # Rio (3304557): zero dias secos quentes (nem seco, nem quente).
    rj = next(r for r in repo_resultados.salvos if r.municipio_id == 3304557)
    assert rj.frequencia_dias_secos_quentes == 0
    assert rj.intensidade_mm == 0.0

    # Transições: pending → running → completed
    assert repo_execucoes.transicoes == [
        StatusExecucao.RUNNING,
        StatusExecucao.COMPLETED,
    ]
    assert repo_execucoes.execucoes[execucao_id].status == StatusExecucao.COMPLETED
    assert repo_execucoes.execucoes[execucao_id].concluido_em is not None


@pytest.mark.asyncio
async def test_handler_falha_transiciona_para_failed_e_propaga() -> None:
    execucao_id = "exec_fail"
    repo_execucoes = _RepoExecucoesFake(
        execucoes={
            execucao_id: Execucao(
                id=execucao_id,
                cenario="rcp45",
                variavel="pr+tas+evap",
                arquivo_origem="/tmp/pr.nc",
                tipo="estresse_hidrico",
                parametros={},
                status=StatusExecucao.PENDING,
                criado_em=datetime(2026, 1, 1, tzinfo=UTC),
                concluido_em=None,
                job_id=None,
            )
        }
    )
    repo_resultados = _RepoResultadosFake()

    class _LeitorQuebrado:
        def abrir(
            self,
            caminho_pr: Path,
            caminho_tas: Path,
            caminho_evap: Path,
        ) -> DadosClimaticosMultiVariaveis:
            raise RuntimeError("arquivo corrompido")

    class _AgregadorIgnorado:
        def agregar_por_municipio(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            raise AssertionError("não deveria ser chamado")

    handler = criar_handler_estresse_hidrico(
        leitor=_LeitorQuebrado(),  # type: ignore[arg-type]
        agregador=_AgregadorIgnorado(),  # type: ignore[arg-type]
        repositorio_execucoes=repo_execucoes,  # type: ignore[arg-type]
        repositorio_resultados=repo_resultados,  # type: ignore[arg-type]
    )

    payload = {
        "execucao_id": execucao_id,
        "arquivo_pr": "/tmp/pr.nc",
        "arquivo_tas": "/tmp/tas.nc",
        "arquivo_evap": "/tmp/evap.nc",
        "cenario": "rcp45",
        "limiar_pr_mm_dia": 1.0,
        "limiar_tas_c": 30.0,
    }
    with pytest.raises(RuntimeError, match="arquivo corrompido"):
        await handler(payload)

    assert repo_execucoes.execucoes[execucao_id].status == StatusExecucao.FAILED
    assert repo_resultados.salvos == []
