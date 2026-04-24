"""Handler de job do pipeline de estresse hídrico (Slice 15).

Responsabilidade: consome um :class:`Job` ``processar_estresse_hidrico``,
executa o pipeline completo (ler → agregar → calcular → persistir) e
atualiza a :class:`Execucao` associada para ``completed``/``failed``.

Arquitetura: o handler é uma *closure* criada por :func:`criar_handler_estresse_hidrico`.
Todas as dependências (leitor, agregador, repositórios) são injetadas na
fábrica; o CLI do worker monta o wiring.

ADR-005: imports deste módulo restritos a :mod:`stdlib`, :mod:`domain`,
:mod:`application` e :mod:`pandas`/`numpy`. Zero dependência de
``xarray``/``geopandas`` — elas ficam atrás das portas.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
    calcular_indices_anuais_estresse_hidrico,
)
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.portas.agregador_espacial import AgregadorEspacial
from climate_risk.domain.portas.leitor_multivariavel import LeitorMultiVariavel
from climate_risk.domain.portas.repositorio_resultado_estresse_hidrico import (
    RepositorioResultadoEstresseHidrico,
)
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = ["HandlerEstresseHidrico", "criar_handler_estresse_hidrico"]

logger = logging.getLogger(__name__)

HandlerEstresseHidrico = Callable[[dict[str, Any]], Awaitable[None]]


def criar_handler_estresse_hidrico(
    *,
    leitor: LeitorMultiVariavel,
    agregador: AgregadorEspacial,
    repositorio_execucoes: RepositorioExecucoes,
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
) -> HandlerEstresseHidrico:
    """Fábrica do handler ``processar_estresse_hidrico``.

    Returns:
        Coroutine ``(payload) -> None`` pronta para registro no Worker.
    """

    async def _handler(payload: dict[str, Any]) -> None:
        await _processar(
            payload,
            leitor=leitor,
            agregador=agregador,
            repositorio_execucoes=repositorio_execucoes,
            repositorio_resultados=repositorio_resultados,
        )

    return _handler


async def _processar(
    payload: dict[str, Any],
    *,
    leitor: LeitorMultiVariavel,
    agregador: AgregadorEspacial,
    repositorio_execucoes: RepositorioExecucoes,
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
) -> None:
    execucao_id = str(payload["execucao_id"])
    cenario = str(payload["cenario"])
    params = ParametrosIndicesEstresseHidrico(
        limiar_pr_mm_dia=float(payload["limiar_pr_mm_dia"]),
        limiar_tas_c=float(payload["limiar_tas_c"]),
    )

    execucao = await _carregar_execucao(repositorio_execucoes, execucao_id)
    execucao = await _transicionar(
        repositorio_execucoes, execucao, StatusExecucao.RUNNING, concluido=False
    )

    try:
        dados = leitor.abrir(
            caminho_pr=Path(payload["arquivo_pr"]),
            caminho_tas=Path(payload["arquivo_tas"]),
            caminho_evap=Path(payload["arquivo_evap"]),
        )
        df_pr = agregador.agregar_por_municipio(dados.precipitacao_diaria_mm, "pr")
        df_tas = agregador.agregar_por_municipio(dados.temperatura_diaria_c, "tas")
        df_evap = agregador.agregar_por_municipio(dados.evaporacao_diaria_mm, "evap")

        df_combinado = _combinar_dataframes(df_pr, df_tas, df_evap)
        resultados = _calcular_resultados_por_municipio(
            df_combinado,
            execucao_id=execucao_id,
            cenario=cenario,
            params=params,
        )

        await repositorio_resultados.salvar_lote(resultados)
    except Exception:
        await _transicionar(repositorio_execucoes, execucao, StatusExecucao.FAILED, concluido=True)
        raise

    await _transicionar(repositorio_execucoes, execucao, StatusExecucao.COMPLETED, concluido=True)
    logger.info(
        "HandlerEstresseHidrico concluído execucao_id=%s linhas=%d",
        execucao_id,
        len(resultados),
    )


def _combinar_dataframes(
    df_pr: pd.DataFrame,
    df_tas: pd.DataFrame,
    df_evap: pd.DataFrame,
) -> pd.DataFrame:
    """Merge inner por ``(municipio_id, data)`` expondo colunas ``pr``/``tas``/``evap``.

    Cada DataFrame de entrada tem ``[municipio_id, data, valor, nome_variavel]``.
    Municípios fora da interseção das três variáveis são descartados (é o
    caso esperado quando a grade de ``evap`` tem cobertura diferente de
    ``pr``/``tas``).
    """
    if df_pr.empty or df_tas.empty or df_evap.empty:
        return pd.DataFrame(columns=["municipio_id", "data", "pr", "tas", "evap"])

    def _renomear(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
        return df[["municipio_id", "data", "valor"]].rename(columns={"valor": coluna})

    merged = _renomear(df_pr, "pr").merge(
        _renomear(df_tas, "tas"),
        on=["municipio_id", "data"],
        how="inner",
    )
    merged = merged.merge(
        _renomear(df_evap, "evap"),
        on=["municipio_id", "data"],
        how="inner",
    )
    return merged


def _calcular_resultados_por_municipio(
    df: pd.DataFrame,
    *,
    execucao_id: str,
    cenario: str,
    params: ParametrosIndicesEstresseHidrico,
) -> list[ResultadoEstresseHidrico]:
    if df.empty:
        return []
    # O agregador devolve ``municipio_id`` como string (vem do shapefile IBGE).
    # A entidade persiste como int; cast explícito abaixo.
    df = df.copy()
    df["ano"] = pd.to_datetime(df["data"]).dt.year
    agora = utc_now()

    resultados: list[ResultadoEstresseHidrico] = []
    for (municipio_id_raw, ano), sub in df.groupby(["municipio_id", "ano"], sort=True):
        indices = calcular_indices_anuais_estresse_hidrico(
            pr_mm_dia=np.asarray(sub["pr"].to_numpy(), dtype=np.float64),
            tas_c=np.asarray(sub["tas"].to_numpy(), dtype=np.float64),
            evap_mm_dia=np.asarray(sub["evap"].to_numpy(), dtype=np.float64),
            params=params,
        )
        resultados.append(
            ResultadoEstresseHidrico(
                id=gerar_id("reh"),
                execucao_id=execucao_id,
                municipio_id=int(municipio_id_raw),
                ano=int(ano),
                cenario=cenario,
                frequencia_dias_secos_quentes=indices.dias_secos_quentes,
                intensidade_mm=indices.intensidade_estresse,
                criado_em=agora,
            )
        )
    return resultados


async def _carregar_execucao(repo: RepositorioExecucoes, execucao_id: str) -> Execucao:
    execucao = await repo.buscar_por_id(execucao_id)
    if execucao is None:
        raise ErroEntidadeNaoEncontrada(entidade="Execucao", identificador=execucao_id)
    return execucao


async def _transicionar(
    repo: RepositorioExecucoes,
    execucao: Execucao,
    novo_status: str,
    *,
    concluido: bool,
) -> Execucao:
    agora = datetime.now(UTC)
    atualizada = Execucao(
        id=execucao.id,
        cenario=execucao.cenario,
        variavel=execucao.variavel,
        arquivo_origem=execucao.arquivo_origem,
        tipo=execucao.tipo,
        parametros=execucao.parametros,
        status=novo_status,
        criado_em=execucao.criado_em,
        concluido_em=agora if concluido else execucao.concluido_em,
        job_id=execucao.job_id,
    )
    await repo.salvar(atualizada)
    return atualizada
