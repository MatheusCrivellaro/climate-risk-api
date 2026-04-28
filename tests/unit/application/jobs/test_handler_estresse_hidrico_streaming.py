"""Testes do streaming do handler de estresse hídrico (Slice 21 / 22 / 23).

Foco em: idempotência (deletar parciais), batches, interseção de cobertura
municipal entre as 3 grades (Slice 22 / ADR-014), iteração filtrada via
``zip`` (Slice 23 / ADR-015), logging estruturado, marcação final de
execução.
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
    """Agregador fake com cobertura por variável configurável.

    Slice 22: o handler calcula interseção das 3 grades via
    :meth:`municipios_mapeados`. Slice 23: a iteração propriamente dita
    usa :meth:`iterar_por_municipio` com ``municipios_alvo=interseção``,
    consumida via ``zip``. Esta fake expõe ambos por variável
    (``"pr"`` / ``"tas"`` / ``"evap"``), permitindo simular grades com
    coberturas distintas para testar o caminho de divergência sem
    precisar do shapefile real.

    Também registra as chamadas a :meth:`iterar_por_municipio` e
    :meth:`serie_de_municipio` para os testes verificarem que o handler
    usa o iterador filtrado e **não** chama ``serie_de_municipio`` em loop.
    """

    cobertura: dict[str, set[int]]
    datas: np.ndarray
    chamadas_iterar: list[tuple[str, set[int] | None]] = field(default_factory=list)
    chamadas_serie_de_municipio: list[tuple[str, int]] = field(default_factory=list)

    @classmethod
    def uniforme(cls, n_municipios: int, datas: np.ndarray) -> _AgregadorFake:
        ids = {1000000 + i for i in range(n_municipios)}
        return cls(cobertura={"pr": ids, "tas": ids, "evap": ids}, datas=datas)

    def municipios_mapeados(self, dados: xr.DataArray) -> set[int]:
        return set(self.cobertura[self._variavel(dados)])

    def serie_de_municipio(
        self, dados: xr.DataArray, municipio_id: int
    ) -> tuple[np.ndarray, np.ndarray]:
        variavel = self._variavel(dados)
        self.chamadas_serie_de_municipio.append((variavel, municipio_id))
        if municipio_id not in self.cobertura[variavel]:
            raise KeyError(f"Município {municipio_id} fora da grade {variavel}")
        return self.datas.copy(), self._serie_constante(variavel)

    def iterar_por_municipio(
        self,
        dados: xr.DataArray,
        *,
        municipios_alvo: set[int] | None = None,
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        variavel = self._variavel(dados)
        self.chamadas_iterar.append(
            (variavel, set(municipios_alvo) if municipios_alvo is not None else None)
        )
        ids_mapeados = self.cobertura[variavel]
        if municipios_alvo is not None:
            ids = ids_mapeados & municipios_alvo
        else:
            ids = ids_mapeados
        for municipio_id in sorted(ids):
            yield municipio_id, self.datas.copy(), self._serie_constante(variavel)

    def agregar_por_municipio(
        self, dados: xr.DataArray, nome_variavel: str
    ) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def _variavel(self, dados: xr.DataArray) -> str:
        nome = str(dados.name)
        if nome not in self.cobertura:
            raise AssertionError(f"variável inesperada: {nome!r}")
        return nome

    def _serie_constante(self, variavel: str) -> np.ndarray:
        if variavel == "pr":
            return np.zeros_like(self.datas, dtype=np.float64)
        if variavel == "tas":
            return np.full_like(self.datas, 35.0, dtype=np.float64)
        if variavel == "evap":
            return np.full_like(self.datas, 2.0, dtype=np.float64)
        raise AssertionError(variavel)


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
    cobertura: dict[str, set[int]] | None = None,
) -> Any:
    datas = pd.date_range("2030-01-01", periods=2, freq="D").to_numpy()
    leitor = _LeitorFake(pr=_da("pr"), tas=_da("tas"), evap=_da("evap"))
    agregador: Any = (
        _AgregadorFake(cobertura=cobertura, datas=datas)
        if cobertura is not None
        else _AgregadorFake.uniforme(n_municipios=n_municipios, datas=datas)
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
async def test_handler_processa_apenas_interseccao_de_municipios() -> None:
    """Slice 22: pr/tas cobrem {1,2,3}; evap cobre {2,3,4}. Processa só {2,3}."""
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_int": _execucao("exec_int")})
    handler = _construir_handler(
        n_municipios=0,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
        cobertura={"pr": {1, 2, 3}, "tas": {1, 2, 3}, "evap": {2, 3, 4}},
    )

    await handler(_payload("exec_int"))

    municipios_persistidos = {r.municipio_id for r in repo_resultados.salvos}
    assert municipios_persistidos == {2, 3}


@pytest.mark.asyncio
async def test_handler_loga_warning_com_municipios_divergentes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warning estruturado tem contagens e amostras corretas por categoria."""
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_w": _execucao("exec_w")})
    handler = _construir_handler(
        n_municipios=0,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
        cobertura={"pr": {1, 2, 3}, "tas": {1, 2, 3}, "evap": {2, 3, 4}},
    )

    with caplog.at_level(logging.WARNING, logger=modulo_handlers.logger.name):
        await handler(_payload("exec_w"))

    warnings_div = [rec for rec in caplog.records if "divergentes entre grades" in rec.message]
    assert len(warnings_div) == 1, "esperava um único warning de divergência"
    rec = warnings_div[0]
    assert getattr(rec, "execucao_id", None) == "exec_w"
    assert getattr(rec, "total_pulados", None) == 2
    assert getattr(rec, "total_processados", None) == 2

    em_pr_tas = getattr(rec, "em_pr_tas_mas_nao_evap", {})
    assert em_pr_tas["count"] == 1
    assert em_pr_tas["amostra"] == [1]

    so_evap = getattr(rec, "so_em_evap", {})
    assert so_evap["count"] == 1
    assert so_evap["amostra"] == [4]


@pytest.mark.asyncio
async def test_handler_nao_loga_warning_quando_grades_concordam(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_ok": _execucao("exec_ok")})
    handler = _construir_handler(
        n_municipios=3,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    with caplog.at_level(logging.WARNING, logger=modulo_handlers.logger.name):
        await handler(_payload("exec_ok"))

    warnings_div = [rec for rec in caplog.records if "divergentes entre grades" in rec.message]
    assert warnings_div == []


@pytest.mark.asyncio
async def test_handler_processa_todos_quando_grades_iguais() -> None:
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_e": _execucao("exec_e")})
    handler = _construir_handler(
        n_municipios=3,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
    )

    await handler(_payload("exec_e"))

    municipios_persistidos = {r.municipio_id for r in repo_resultados.salvos}
    assert municipios_persistidos == {1000000, 1000001, 1000002}


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


# ---------------------------------------------------------------------
# Slice 23 / ADR-015: handler usa zip de iteradores filtrados
# ---------------------------------------------------------------------


def _construir_handler_e_agregador(
    *,
    n_municipios: int,
    repo_resultados: _RepoResultadosFake,
    repo_execucoes: _RepoExecucoesFake,
    cobertura: dict[str, set[int]] | None = None,
) -> tuple[Any, _AgregadorFake]:
    datas = pd.date_range("2030-01-01", periods=2, freq="D").to_numpy()
    leitor = _LeitorFake(pr=_da("pr"), tas=_da("tas"), evap=_da("evap"))
    agregador: _AgregadorFake = (
        _AgregadorFake(cobertura=cobertura, datas=datas)
        if cobertura is not None
        else _AgregadorFake.uniforme(n_municipios=n_municipios, datas=datas)
    )
    handler = criar_handler_estresse_hidrico(
        leitor=leitor,  # type: ignore[arg-type]
        agregador=agregador,
        repositorio_execucoes=repo_execucoes,  # type: ignore[arg-type]
        repositorio_resultados=repo_resultados,  # type: ignore[arg-type]
    )
    return handler, agregador


@pytest.mark.asyncio
async def test_handler_usa_iterar_por_municipio_filtrado_e_nao_serie_de_municipio() -> None:
    """Handler chama iterar_por_municipio 3x (uma por variável) com municipios_alvo=interseção
    e **não** chama serie_de_municipio em loop por município."""
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_iter": _execucao("exec_iter")})
    handler, agregador = _construir_handler_e_agregador(
        n_municipios=0,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
        cobertura={"pr": {1, 2, 3}, "tas": {1, 2, 3}, "evap": {2, 3, 4}},
    )

    await handler(_payload("exec_iter"))

    # Exatamente 3 chamadas a iterar_por_municipio, uma por variável.
    variaveis_chamadas = [v for v, _ in agregador.chamadas_iterar]
    assert sorted(variaveis_chamadas) == ["evap", "pr", "tas"]

    # As 3 chamadas receberam o mesmo conjunto-alvo, igual à interseção.
    alvos = [alvo for _, alvo in agregador.chamadas_iterar]
    assert all(a == {2, 3} for a in alvos)

    # No caminho normal (sem fallback de divergência), serie_de_municipio
    # NÃO é chamado — perderia a localidade do streaming dask.
    assert agregador.chamadas_serie_de_municipio == []


@pytest.mark.asyncio
async def test_handler_iteradores_sincronizados_via_zip() -> None:
    """3 iteradores filtrados pela mesma interseção produzem o mesmo município
    a cada step do zip; o handler conclui sem RuntimeError de inconsistência."""
    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_zip": _execucao("exec_zip")})
    handler, _ = _construir_handler_e_agregador(
        n_municipios=0,
        repo_resultados=repo_resultados,
        repo_execucoes=repo_execucoes,
        cobertura={"pr": {1, 2, 3}, "tas": {1, 2, 3}, "evap": {2, 3, 4}},
    )

    await handler(_payload("exec_zip"))

    municipios_persistidos = {r.municipio_id for r in repo_resultados.salvos}
    assert municipios_persistidos == {2, 3}


@pytest.mark.asyncio
async def test_handler_levanta_erro_se_iteradores_dessincronizam_apesar_do_filtro() -> None:
    """Simula bug futuro: o iterador de uma variável devolve municípios fora
    da ordem ou ID divergente. Handler levanta RuntimeError descritivo."""

    @dataclass
    class _AgregadorDessincronizado:
        cobertura: dict[str, set[int]]
        datas: np.ndarray

        def municipios_mapeados(self, dados: xr.DataArray) -> set[int]:
            return set(self.cobertura[str(dados.name)])

        def serie_de_municipio(
            self, dados: xr.DataArray, municipio_id: int
        ) -> tuple[np.ndarray, np.ndarray]:  # pragma: no cover
            raise NotImplementedError

        def iterar_por_municipio(
            self,
            dados: xr.DataArray,
            *,
            municipios_alvo: set[int] | None = None,
        ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
            nome = str(dados.name)
            ids = sorted(self.cobertura[nome])
            # Bug simulado: a variável "evap" quebra a ordem ascendente.
            if nome == "evap":
                ids = list(reversed(ids))
            for mun in ids:
                yield mun, self.datas.copy(), np.zeros_like(self.datas, dtype=np.float64)

        def agregar_por_municipio(
            self, dados: xr.DataArray, nome_variavel: str
        ) -> pd.DataFrame:  # pragma: no cover
            raise NotImplementedError

    repo_resultados = _RepoResultadosFake()
    repo_execucoes = _RepoExecucoesFake(execucoes={"exec_bug": _execucao("exec_bug")})

    datas = pd.date_range("2030-01-01", periods=2, freq="D").to_numpy()
    leitor = _LeitorFake(pr=_da("pr"), tas=_da("tas"), evap=_da("evap"))
    agregador = _AgregadorDessincronizado(
        cobertura={"pr": {1, 2}, "tas": {1, 2}, "evap": {1, 2}}, datas=datas
    )
    handler = criar_handler_estresse_hidrico(
        leitor=leitor,  # type: ignore[arg-type]
        agregador=agregador,  # type: ignore[arg-type]
        repositorio_execucoes=repo_execucoes,  # type: ignore[arg-type]
        repositorio_resultados=repo_resultados,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="Inconsistência de iteração com filtro"):
        await handler(_payload("exec_bug"))
