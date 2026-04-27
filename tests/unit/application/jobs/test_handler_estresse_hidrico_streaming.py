"""Testes do streaming do handler de estresse hídrico (Slice 21 / ADR-013).

Foco em: idempotência (deletar parciais), batches, sincronização entre 3
iteradores, logging estruturado, marcação final de execução.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.application.jobs import handlers_estresse_hidrico as modulo_handlers
from climate_risk.application.jobs.handlers_estresse_hidrico import (
    BATCH_SIZE,
    criar_handler_estresse_hidrico,
)
from climate_risk.domain.entidades.dados_multivariaveis import DadosClimaticosMultiVariaveis
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)


def _da(name: str) -> xr.DataArray:
    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    return xr.DataArray(np.array([0.0, 0.0]), dims=["time"], coords={"time": tempo}, name=name)


@dataclass
class _LeitorFake:
    pr: xr.DataArray
    tas: xr.DataArray
    evap: xr.DataArray

    def abrir(
        self,
        caminho_pr: Path,
        caminho_tas: Path,
        caminho_evap: Path,
    ) -> DadosClimaticosMultiVariaveis:
        return DadosClimaticosMultiVariaveis(
            precipitacao_diaria_mm=self.pr,
            temperatura_diaria_c=self.tas,
            evaporacao_diaria_mm=self.evap,
            tempo=pd.DatetimeIndex(self.pr["time"].values),
            cenario="rcp45",
        )


@dataclass
class _AgregadorFake:
    """Yield N municípios sequenciais com séries idênticas para todas as variáveis."""

    n_municipios: int
    datas: np.ndarray

    def iterar_por_municipio(
        self, dados: xr.DataArray
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        for i in range(self.n_municipios):
            municipio_id = 1000000 + i
            # Valores sintéticos: pr=0 (sempre seco), tas=35 (sempre quente),
            # evap=2 (déficit constante 2.0). Resultado anual deterministico.
            if dados.name == "pr":
                serie = np.zeros_like(self.datas, dtype=np.float64)
            elif dados.name == "tas":
                serie = np.full_like(self.datas, 35.0, dtype=np.float64)
            elif dados.name == "evap":
                serie = np.full_like(self.datas, 2.0, dtype=np.float64)
            else:
                raise AssertionError(f"variável inesperada: {dados.name!r}")
            yield municipio_id, self.datas.copy(), serie

    def agregar_por_municipio(
        self, dados: xr.DataArray, nome_variavel: str
    ) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError


@dataclass
class _AgregadorDessincronizado:
    """Yield ordens divergentes entre as 3 variáveis para acionar a guarda."""

    def iterar_por_municipio(
        self, dados: xr.DataArray
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        datas = pd.date_range("2030-01-01", periods=2, freq="D").to_numpy()
        if dados.name == "pr":
            yield 1, datas, np.array([0.0, 0.0])
            yield 2, datas, np.array([0.0, 0.0])
        elif dados.name == "tas":
            # Ordem trocada de propósito.
            yield 2, datas, np.array([35.0, 35.0])
            yield 1, datas, np.array([35.0, 35.0])
        else:  # evap
            yield 1, datas, np.array([2.0, 2.0])
            yield 2, datas, np.array([2.0, 2.0])

    def agregar_por_municipio(
        self, dados: xr.DataArray, nome_variavel: str
    ) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError


@dataclass
class _RepoExecucoesFake:
    execucoes: dict[str, Execucao] = field(default_factory=dict)
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
    chamadas_salvar_lote: list[int] = field(default_factory=list)
    chamadas_deletar: list[str] = field(default_factory=list)

    async def salvar_lote(self, resultados: Any) -> None:
        lista = list(resultados)
        self.chamadas_salvar_lote.append(len(lista))
        self.salvos.extend(lista)

    async def listar(self, **_: Any) -> list[ResultadoEstresseHidrico]:
        return list(self.salvos)

    async def contar(self, **_: Any) -> int:
        return len(self.salvos)

    async def deletar_por_execucao(self, execucao_id: str) -> int:
        self.chamadas_deletar.append(execucao_id)
        antes = len(self.salvos)
        self.salvos = [r for r in self.salvos if r.execucao_id != execucao_id]
        return antes - len(self.salvos)


def _execucao(execucao_id: str) -> Execucao:
    return Execucao(
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


def _construir_handler(
    *,
    n_municipios: int,
    repo_resultados: _RepoResultadosFake,
    repo_execucoes: _RepoExecucoesFake,
    agregador_dessincronizado: bool = False,
) -> Any:
    datas = pd.date_range("2030-01-01", periods=2, freq="D").to_numpy()
    leitor = _LeitorFake(pr=_da("pr"), tas=_da("tas"), evap=_da("evap"))
    agregador: Any = (
        _AgregadorDessincronizado()
        if agregador_dessincronizado
        else _AgregadorFake(n_municipios=n_municipios, datas=datas)
    )
    return criar_handler_estresse_hidrico(
        leitor=leitor,  # type: ignore[arg-type]
        agregador=agregador,
        repositorio_execucoes=repo_execucoes,  # type: ignore[arg-type]
        repositorio_resultados=repo_resultados,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_handler_chama_deletar_por_execucao_no_inicio() -> None:
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_x": _execucao("exec_x")})
    handler = _construir_handler(
        n_municipios=3,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    await handler(_payload("exec_x"))

    assert repo_resultados.chamadas_deletar == ["exec_x"]


@pytest.mark.asyncio
async def test_handler_persiste_em_batches_nao_em_uma_chamada(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """250 municípios com BATCH_SIZE=100 → pelo menos 3 commits (3 batches)."""
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_b": _execucao("exec_b")})
    handler = _construir_handler(
        n_municipios=250,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    await handler(_payload("exec_b"))

    # 250 municípios x 1 ano = 250 resultados. BATCH_SIZE=100 -> >= 2 chamadas
    # (slice doc requisita "pelo menos 2"); na prática, 3 (100, 100, 50).
    assert len(repo_resultados.chamadas_salvar_lote) >= 2
    assert sum(repo_resultados.chamadas_salvar_lote) == 250
    # Nenhum batch parcial deve exceder o BATCH_SIZE.
    assert max(repo_resultados.chamadas_salvar_lote) <= BATCH_SIZE


@pytest.mark.asyncio
async def test_handler_levanta_erro_descritivo_em_iteradores_dessincronizados() -> None:
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_d": _execucao("exec_d")})
    handler = _construir_handler(
        n_municipios=2,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
        agregador_dessincronizado=True,
    )

    with pytest.raises(RuntimeError, match="Inconsistência de iteração"):
        await handler(_payload("exec_d"))

    # Execução transiciona para failed.
    assert repo_execucoes.execucoes["exec_d"].status == StatusExecucao.FAILED


@pytest.mark.asyncio
async def test_handler_marca_execucao_concluida_apos_processar() -> None:
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_c": _execucao("exec_c")})
    handler = _construir_handler(
        n_municipios=2,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    await handler(_payload("exec_c"))

    assert repo_execucoes.execucoes["exec_c"].status == StatusExecucao.COMPLETED
    assert repo_execucoes.execucoes["exec_c"].concluido_em is not None


@pytest.mark.asyncio
async def test_handler_log_progresso_inclui_metadados_estruturados(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Após N municípios, há log com ``execucao_id``/``municipios_processados`` em ``extra``."""
    # Reduz LOG_INTERVALO para evitar precisar de muitos municípios no teste.
    monkeypatch.setattr(modulo_handlers, "LOG_INTERVALO", 2)

    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_log": _execucao("exec_log")})
    handler = _construir_handler(
        n_municipios=4,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    with caplog.at_level(logging.INFO, logger=modulo_handlers.logger.name):
        await handler(_payload("exec_log"))

    progressos = [
        rec
        for rec in caplog.records
        if rec.message.startswith("Progresso pipeline estresse hídrico")
    ]
    assert progressos, "esperava ao menos um log de progresso"
    primeiro = progressos[0]
    assert getattr(primeiro, "execucao_id", None) == "exec_log"
    assert isinstance(getattr(primeiro, "municipios_processados", None), int)
    assert isinstance(getattr(primeiro, "resultados_persistidos", None), int)


@pytest.mark.asyncio
async def test_handler_idempotente_apaga_parciais_antes_de_recomecar() -> None:
    """Pré-popular um resultado parcial; após o handler, ele é substituído."""
    parcial = ResultadoEstresseHidrico(
        id="reh_velho",
        execucao_id="exec_idem",
        municipio_id=1000000,
        ano=2030,
        cenario="rcp45",
        frequencia_dias_secos_quentes=999,
        intensidade_mm_dia=999.9,
        criado_em=datetime(2030, 1, 1, tzinfo=UTC),
    )
    repo_resultados = _RepoResultadosFake(salvos=[parcial])
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_idem": _execucao("exec_idem")})
    handler = _construir_handler(
        n_municipios=1,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    await handler(_payload("exec_idem"))

    # O parcial original sumiu; só sobra o resultado novo.
    assert all(r.id != "reh_velho" for r in repo_resultados.salvos)
    assert len(repo_resultados.salvos) == 1
    novo = repo_resultados.salvos[0]
    assert novo.execucao_id == "exec_idem"
    # Valores recalculados — não os 999 do parcial.
    assert novo.frequencia_dias_secos_quentes == 2  # 2 dias secos quentes (todos)
    assert novo.intensidade_mm_dia == pytest.approx(2.0)  # déficit constante 2.0
