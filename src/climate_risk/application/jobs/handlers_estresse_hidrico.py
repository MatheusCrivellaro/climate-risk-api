"""Handler de job do pipeline de estresse hídrico (Slice 15 / Slice 21).

Responsabilidade: consome um :class:`Job` ``processar_estresse_hidrico``,
executa o pipeline completo (ler → agregar → calcular → persistir) e
atualiza a :class:`Execucao` associada para ``completed``/``failed``.

Arquitetura: o handler é uma *closure* criada por :func:`criar_handler_estresse_hidrico`.
Todas as dependências (leitor, agregador, repositórios) são injetadas na
fábrica; o CLI do worker monta o wiring.

A partir da Slice 21 o pipeline é **streaming**: o agregador é consumido
município a município via :meth:`AgregadorEspacial.iterar_por_municipio`
e os resultados são persistidos em batches pequenos (``BATCH_SIZE``).
Idempotência: cada execução começa apagando resultados parciais
existentes (se houver) — assim retries não esbarram em ``UniqueConstraint``.
Ver ADR-013.

ADR-005: imports deste módulo restritos a :mod:`stdlib`, :mod:`domain`,
:mod:`application` e :mod:`pandas`/`numpy`. Zero dependência de
``xarray``/``geopandas`` — elas ficam atrás das portas.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

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

__all__ = [
    "BATCH_SIZE",
    "LOG_INTERVALO",
    "HandlerEstresseHidrico",
    "criar_handler_estresse_hidrico",
    "criar_handler_estresse_hidrico_pasta",
]

logger = logging.getLogger(__name__)

HandlerEstresseHidrico = Callable[[dict[str, Any]], Awaitable[None]]

# Município por batch de inserção. Mantém o write amplification baixo sem
# explodir a memória: 100 municípios x ~30 anos = ~3000 linhas por commit.
BATCH_SIZE = 100

# Frequência (em municípios) dos logs estruturados de progresso.
LOG_INTERVALO = 100


def criar_handler_estresse_hidrico(
    *,
    leitor: LeitorMultiVariavel,
    agregador: AgregadorEspacial,
    repositorio_execucoes: RepositorioExecucoes,
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
) -> HandlerEstresseHidrico:
    """Fábrica do handler ``processar_estresse_hidrico`` (arquivo único)."""

    async def _handler(payload: dict[str, Any]) -> None:
        await _processar(
            payload,
            leitor=leitor,
            agregador=agregador,
            repositorio_execucoes=repositorio_execucoes,
            repositorio_resultados=repositorio_resultados,
        )

    return _handler


def criar_handler_estresse_hidrico_pasta(
    *,
    leitor: LeitorMultiVariavel,
    agregador: AgregadorEspacial,
    repositorio_execucoes: RepositorioExecucoes,
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
) -> HandlerEstresseHidrico:
    """Fábrica do handler ``processar_estresse_hidrico_pasta`` (Slice 17)."""

    async def _handler(payload: dict[str, Any]) -> None:
        await _processar_de_pastas(
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
        await _limpar_parciais(repositorio_resultados, execucao_id)
        dados = leitor.abrir(
            caminho_pr=Path(payload["arquivo_pr"]),
            caminho_tas=Path(payload["arquivo_tas"]),
            caminho_evap=Path(payload["arquivo_evap"]),
        )
        total = await _processar_streaming(
            agregador=agregador,
            repositorio_resultados=repositorio_resultados,
            dados_pr=dados.precipitacao_diaria_mm,
            dados_tas=dados.temperatura_diaria_c,
            dados_evap=dados.evaporacao_diaria_mm,
            execucao_id=execucao_id,
            cenario=cenario,
            params=params,
        )
    except Exception:
        await _transicionar(repositorio_execucoes, execucao, StatusExecucao.FAILED, concluido=True)
        raise

    await _transicionar(repositorio_execucoes, execucao, StatusExecucao.COMPLETED, concluido=True)
    logger.info(
        "Pipeline estresse hídrico concluído",
        extra={
            "execucao_id": execucao_id,
            "municipios_processados": total.municipios,
            "resultados_persistidos": total.resultados,
        },
    )


async def _processar_de_pastas(
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
        await _limpar_parciais(repositorio_resultados, execucao_id)
        dados = leitor.abrir_de_pastas(
            pasta_pr=Path(payload["pasta_pr"]),
            pasta_tas=Path(payload["pasta_tas"]),
            pasta_evap=Path(payload["pasta_evap"]),
            cenario_esperado=cenario,
        )
        total = await _processar_streaming(
            agregador=agregador,
            repositorio_resultados=repositorio_resultados,
            dados_pr=dados.precipitacao_diaria_mm,
            dados_tas=dados.temperatura_diaria_c,
            dados_evap=dados.evaporacao_diaria_mm,
            execucao_id=execucao_id,
            cenario=cenario,
            params=params,
        )
    except Exception:
        await _transicionar(repositorio_execucoes, execucao, StatusExecucao.FAILED, concluido=True)
        raise

    await _transicionar(repositorio_execucoes, execucao, StatusExecucao.COMPLETED, concluido=True)
    logger.info(
        "Pipeline estresse hídrico (pasta) concluído",
        extra={
            "execucao_id": execucao_id,
            "municipios_processados": total.municipios,
            "resultados_persistidos": total.resultados,
        },
    )


class _Totais:
    """Acumulador simples de contadores ao longo do pipeline."""

    __slots__ = ("municipios", "resultados")

    def __init__(self) -> None:
        self.municipios = 0
        self.resultados = 0


async def _processar_streaming(
    *,
    agregador: AgregadorEspacial,
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
    dados_pr: Any,
    dados_tas: Any,
    dados_evap: Any,
    execucao_id: str,
    cenario: str,
    params: ParametrosIndicesEstresseHidrico,
) -> _Totais:
    """Núcleo streaming: itera 3 variáveis em paralelo e persiste em batches.

    Levanta ``RuntimeError`` se as 3 iterações divergirem em ``municipio_id``
    (sinal de não-determinismo no agregador, que quebraria o pareamento).
    """
    iter_pr = agregador.iterar_por_municipio(dados_pr)
    iter_tas = agregador.iterar_por_municipio(dados_tas)
    iter_evap = agregador.iterar_por_municipio(dados_evap)

    batch: list[ResultadoEstresseHidrico] = []
    totais = _Totais()

    for tripla in _zip_iteradores(iter_pr, iter_tas, iter_evap):
        (mun_pr, datas, serie_pr), (mun_tas, _, serie_tas), (mun_evap, _, serie_evap) = tripla
        if not (mun_pr == mun_tas == mun_evap):
            raise RuntimeError(
                f"Inconsistência de iteração: pr={mun_pr}, tas={mun_tas}, "
                f"evap={mun_evap}. Verificar determinismo da ordem em "
                "AgregadorEspacial.iterar_por_municipio."
            )

        for ano, indices_anuais in _calcular_por_ano(
            datas=datas,
            serie_pr=serie_pr,
            serie_tas=serie_tas,
            serie_evap=serie_evap,
            params=params,
        ):
            batch.append(
                ResultadoEstresseHidrico(
                    id=gerar_id("reh"),
                    execucao_id=execucao_id,
                    municipio_id=int(mun_pr),
                    ano=int(ano),
                    cenario=cenario,
                    frequencia_dias_secos_quentes=indices_anuais.dias_secos_quentes,
                    intensidade_mm_dia=indices_anuais.intensidade_mm_dia,
                    criado_em=utc_now(),
                )
            )

        totais.municipios += 1

        if len(batch) >= BATCH_SIZE:
            await repositorio_resultados.salvar_lote(batch)
            totais.resultados += len(batch)
            batch = []

        if totais.municipios % LOG_INTERVALO == 0:
            logger.info(
                "Progresso pipeline estresse hídrico",
                extra={
                    "execucao_id": execucao_id,
                    "municipios_processados": totais.municipios,
                    "resultados_persistidos": totais.resultados,
                },
            )

    if batch:
        await repositorio_resultados.salvar_lote(batch)
        totais.resultados += len(batch)

    return totais


def _zip_iteradores(
    iter_pr: Iterator[tuple[int, np.ndarray, np.ndarray]],
    iter_tas: Iterator[tuple[int, np.ndarray, np.ndarray]],
    iter_evap: Iterator[tuple[int, np.ndarray, np.ndarray]],
) -> Iterator[
    tuple[
        tuple[int, np.ndarray, np.ndarray],
        tuple[int, np.ndarray, np.ndarray],
        tuple[int, np.ndarray, np.ndarray],
    ]
]:
    """``zip`` tipado dos três iteradores. Para no primeiro a esgotar."""
    yield from zip(iter_pr, iter_tas, iter_evap, strict=False)


def _calcular_por_ano(
    *,
    datas: np.ndarray,
    serie_pr: np.ndarray,
    serie_tas: np.ndarray,
    serie_evap: np.ndarray,
    params: ParametrosIndicesEstresseHidrico,
) -> Iterator[tuple[int, Any]]:
    """Particiona séries diárias por ano e calcula índices anuais."""
    if len(datas) == 0:
        return
    # ``datas`` chega como ``datetime64[ns]``; extrair ano via numpy evita
    # construir um DatetimeIndex pandas para todo o período.
    anos = datas.astype("datetime64[Y]").astype(int) + 1970
    anos_unicos = np.unique(anos)
    for ano in anos_unicos:
        mascara = anos == ano
        indices = calcular_indices_anuais_estresse_hidrico(
            pr_mm_dia=np.asarray(serie_pr[mascara], dtype=np.float64),
            tas_c=np.asarray(serie_tas[mascara], dtype=np.float64),
            evap_mm_dia=np.asarray(serie_evap[mascara], dtype=np.float64),
            params=params,
        )
        yield int(ano), indices


async def _limpar_parciais(
    repositorio_resultados: RepositorioResultadoEstresseHidrico,
    execucao_id: str,
) -> None:
    deletados = await repositorio_resultados.deletar_por_execucao(execucao_id)
    if deletados > 0:
        logger.info(
            "Resultados parciais anteriores removidos",
            extra={
                "execucao_id": execucao_id,
                "linhas_removidas": deletados,
            },
        )


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
