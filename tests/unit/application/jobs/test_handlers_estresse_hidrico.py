"""Testes unitários do handler ``processar_estresse_hidrico`` (Slice 15 / Slice 21)."""

from __future__ import annotations

from collections.abc import Iterator
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


def _mk_dataarray(valores_por_dia: list[float], tempo: pd.DatetimeIndex, nome: str) -> xr.DataArray:
    arr = np.array(valores_por_dia, dtype=np.float64)
    return xr.DataArray(arr, dims=["time"], coords={"time": tempo}, name=nome)


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
    """Yield séries municipais canned. A chave é o ``name`` do DataArray."""

    series_por_variavel: dict[str, list[tuple[int, np.ndarray, np.ndarray]]]

    def iterar_por_municipio(
        self, dados: xr.DataArray
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        nome = dados.name or ""
        return iter(self.series_por_variavel[str(nome)])

    def agregar_por_municipio(self, dados: xr.DataArray, nome_variavel: str) -> pd.DataFrame:
        raise NotImplementedError("Pipeline streaming (Slice 21) não usa agregar_por_municipio.")


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
    chamadas_salvar_lote: int = 0
    delecoes: list[str] = field(default_factory=list)

    async def salvar_lote(self, resultados: Any) -> None:
        self.chamadas_salvar_lote += 1
        for r in resultados:
            self.salvos.append(r)

    async def listar(self, **_: Any) -> list[ResultadoEstresseHidrico]:
        return list(self.salvos)

    async def contar(self, **_: Any) -> int:
        return len(self.salvos)

    async def deletar_por_execucao(self, execucao_id: str) -> int:
        antes = len(self.salvos)
        self.salvos = [r for r in self.salvos if r.execucao_id != execucao_id]
        deletados = antes - len(self.salvos)
        if deletados > 0:
            self.delecoes.append(execucao_id)
        return deletados


def _fabricar_dados_mult() -> DadosClimaticosMultiVariaveis:
    tempo = pd.date_range("2026-01-01", periods=5, freq="D")
    return DadosClimaticosMultiVariaveis(
        precipitacao_diaria_mm=_mk_dataarray([0.0] * 5, tempo, "pr"),
        temperatura_diaria_c=_mk_dataarray([32.0] * 5, tempo, "tas"),
        evaporacao_diaria_mm=_mk_dataarray([3.0] * 5, tempo, "evap"),
        tempo=pd.DatetimeIndex(tempo),
        cenario="rcp45",
    )


def _fabricar_series_agregador() -> dict[str, list[tuple[int, np.ndarray, np.ndarray]]]:
    """Duas cidades, 5 dias, todos no mesmo ano."""
    tempo = pd.date_range("2026-01-01", periods=5, freq="D").to_numpy()
    sp = 3550308
    rj = 3304557
    return {
        "pr": [
            (sp, tempo, np.array([0.0, 0.0, 0.5, 0.0, 0.2], dtype=np.float64)),
            (rj, tempo, np.array([10.0, 8.0, 5.0, 6.0, 9.0], dtype=np.float64)),
        ],
        "tas": [
            (sp, tempo, np.array([32.0, 33.0, 31.0, 34.0, 35.0], dtype=np.float64)),
            (rj, tempo, np.array([25.0, 24.0, 22.0, 23.0, 20.0], dtype=np.float64)),
        ],
        "evap": [
            (sp, tempo, np.array([3.0, 2.5, 2.0, 4.0, 3.5], dtype=np.float64)),
            (rj, tempo, np.array([1.0, 1.2, 0.8, 1.5, 1.0], dtype=np.float64)),
        ],
    }


def _execucao_pendente(execucao_id: str) -> Execucao:
    return Execucao(
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


@pytest.mark.asyncio
async def test_handler_pipeline_completo_persiste_resultados() -> None:
    execucao_id = "exec_teste_01"
    repo_execucoes = _RepoExecucoesFake(execucoes={execucao_id: _execucao_pendente(execucao_id)})
    repo_resultados = _RepoResultadosFake()
    leitor = _LeitorFake(dados=_fabricar_dados_mult())
    agregador = _AgregadorFake(series_por_variavel=_fabricar_series_agregador())

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
    # Slice 19: intensidade = média (mm/dia) dos déficits nos 5 dias secos quentes.
    # déficits: (3.0-0.0)+(2.5-0.0)+(2.0-0.5)+(4.0-0.0)+(3.5-0.2) = 14.3
    # média: 14.3 / 5 = 2.86
    assert sp.intensidade_mm_dia == pytest.approx(14.3 / 5)

    # Rio (3304557): zero dias secos quentes (nem seco, nem quente).
    rj = next(r for r in repo_resultados.salvos if r.municipio_id == 3304557)
    assert rj.frequencia_dias_secos_quentes == 0
    # Convenção: frequência zero ⇒ intensidade 0.0.
    assert rj.intensidade_mm_dia == 0.0

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
    repo_execucoes = _RepoExecucoesFake(execucoes={execucao_id: _execucao_pendente(execucao_id)})
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
        def iterar_por_municipio(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
            raise AssertionError("não deveria ser chamado")

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
