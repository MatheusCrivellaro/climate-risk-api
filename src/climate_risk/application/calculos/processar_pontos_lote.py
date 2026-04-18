"""Caso de uso :class:`ProcessarPontosLote` (UC-03 — lado assíncrono do fluxo async).

Executado **pelo Worker**, consumindo um :class:`Job` do tipo
``"calcular_pontos"`` enfileirado por
:class:`CriarExecucaoPorPontos`. Orquestra:

1. Carrega a :class:`Execucao` (deve estar em ``pending``).
2. Transiciona para ``running``.
3. Delega o trabalho pesado a :class:`CalcularIndicesPorPontos`
   (Slice 4), passando ``persistir=False`` — ou seja, apenas calcula
   sem criar execução nova nem tocar em resultados.
4. Converte cada :class:`ResultadoPonto` em 8
   :class:`ResultadoIndice` carregando o ``execucao_id`` desta execução.
5. Persiste em lotes de 1000 via :class:`RepositorioResultados`.
6. Finaliza a execução em ``completed`` (ou ``failed`` + re-raise
   quando algo falha, para que o Worker decida sobre retry).

ADR-005: imports restritos a stdlib, :mod:`domain` e outros módulos de
:mod:`application`. Nenhum FastAPI, SQLAlchemy, Pydantic ou ``xarray``.

Notas de design:

- O caso de uso **não** modifica :class:`CalcularIndicesPorPontos`
  (Slice 4): reusamos o caso de uso síncrono como está. Ele devolve
  uma lista de :class:`ResultadoPonto`, que é flatten'ada em
  :class:`ResultadoIndice` aqui — com ``execucao_id`` desta execução.
- Não há limite de pontos neste lado; o controle de limite (100) vive
  apenas no endpoint.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from climate_risk.application.calculos.calcular_por_pontos import (
    UNIDADES_POR_INDICE,
    CalcularIndicesPorPontos,
    ParametrosCalculo,
    PontoEntradaDominio,
)
from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.indices.calculadora import IndicesAnuais, ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.domain.portas.leitor_netcdf import LeitorNetCDF
from climate_risk.domain.portas.repositorios import (
    RepositorioExecucoes,
    RepositorioResultados,
)

__all__ = [
    "ParametrosProcessamentoPontos",
    "ProcessarPontosLote",
    "ResultadoProcessamentoPontos",
]

logger = logging.getLogger(__name__)

# Mesmo tamanho de lote do fluxo CORDEX (ver ProcessarCenarioCordex).
TAMANHO_LOTE_PERSISTENCIA = 1000


@dataclass(frozen=True)
class ParametrosProcessamentoPontos:
    """Entrada do caso de uso assíncrono.

    Todos os campos derivam do payload JSON do :class:`Job`; a conversão
    acontece no handler (ver
    :mod:`climate_risk.application.jobs.handlers_pontos`).
    """

    execucao_id: str
    arquivo_nc: str
    cenario: str
    variavel: str
    pontos: list[PontoEntradaDominio]
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float


@dataclass(frozen=True)
class ResultadoProcessamentoPontos:
    """Sumário devolvido ao handler para log/observabilidade."""

    execucao_id: str
    total_pontos: int
    total_resultados: int


class ProcessarPontosLote:
    """Processa um lote de pontos persistindo :class:`ResultadoIndice`.

    Transições de :class:`Execucao`: ``pending → running → {completed, failed}``.
    """

    def __init__(
        self,
        leitor_netcdf: LeitorNetCDF,
        repositorio_execucoes: RepositorioExecucoes,
        repositorio_resultados: RepositorioResultados,
    ) -> None:
        self._leitor = leitor_netcdf
        self._repo_execucoes = repositorio_execucoes
        self._repo_resultados = repositorio_resultados

    async def executar(self, params: ParametrosProcessamentoPontos) -> ResultadoProcessamentoPontos:
        """Executa o processamento; ver docstring da classe."""
        execucao = await self._carregar_execucao(params.execucao_id)
        execucao = await self._transicionar(execucao, StatusExecucao.RUNNING, concluido=False)

        try:
            sumario = await self._processar(execucao, params)
        except Exception:
            await self._transicionar(execucao, StatusExecucao.FAILED, concluido=True)
            raise

        await self._transicionar(execucao, StatusExecucao.COMPLETED, concluido=True)
        return sumario

    async def _processar(
        self, execucao: Execucao, params: ParametrosProcessamentoPontos
    ) -> ResultadoProcessamentoPontos:
        # Delega ao caso de uso síncrono (Slice 4) com persistir=False.
        # O caso de uso não sabe sobre esta execução — apenas calcula.
        calculadora = CalcularIndicesPorPontos(
            leitor_netcdf=self._leitor,
            repositorio_execucoes=self._repo_execucoes,
            repositorio_resultados=self._repo_resultados,
        )
        parametros_sincronos = ParametrosCalculo(
            arquivo_nc=params.arquivo_nc,
            cenario=params.cenario,
            variavel=params.variavel,
            pontos=params.pontos,
            parametros_indices=params.parametros_indices,
            p95_baseline=params.p95_baseline,
            p95_wet_thr=params.p95_wet_thr,
            persistir=False,
        )
        calculo = await calculadora.executar(parametros_sincronos)

        # Flatten: cada ResultadoPonto → 8 ResultadoIndice (um por índice).
        # O execucao_id do Slice 4 vem vazio; preenchemos aqui.
        lote: list[ResultadoIndice] = []
        total_resultados = 0
        for ponto in calculo.resultados:
            for nome, valor in _achatar_indices(ponto.indices).items():
                lote.append(
                    ResultadoIndice(
                        id=gerar_id("res"),
                        execucao_id=execucao.id,
                        lat=ponto.lat_grid,
                        lon=ponto.lon_grid,
                        lat_input=ponto.lat_input,
                        lon_input=ponto.lon_input,
                        ano=ponto.ano,
                        nome_indice=nome,
                        valor=_nan_para_none(valor),
                        unidade=UNIDADES_POR_INDICE[nome],
                        municipio_id=None,
                    )
                )
                total_resultados += 1
                if len(lote) >= TAMANHO_LOTE_PERSISTENCIA:
                    await self._repo_resultados.salvar_lote(lote)
                    lote = []

        if lote:
            await self._repo_resultados.salvar_lote(lote)

        logger.info(
            "ProcessarPontosLote concluído execucao_id=%s pontos=%d linhas=%d",
            execucao.id,
            len(params.pontos),
            total_resultados,
        )
        return ResultadoProcessamentoPontos(
            execucao_id=execucao.id,
            total_pontos=len(params.pontos),
            total_resultados=total_resultados,
        )

    async def _carregar_execucao(self, execucao_id: str) -> Execucao:
        execucao = await self._repo_execucoes.buscar_por_id(execucao_id)
        if execucao is None:
            raise ErroEntidadeNaoEncontrada(entidade="Execucao", identificador=execucao_id)
        return execucao

    async def _transicionar(
        self, execucao: Execucao, novo_status: str, *, concluido: bool
    ) -> Execucao:
        agora = utc_now()
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
        await self._repo_execucoes.salvar(atualizada)
        return atualizada


def _achatar_indices(indices: IndicesAnuais) -> dict[str, float]:
    """Converte :class:`IndicesAnuais` em ``{nome_indice: valor}``."""
    return {
        "wet_days": float(indices.wet_days),
        "sdii": indices.sdii,
        "rx1day": indices.rx1day,
        "rx5day": indices.rx5day,
        "r20mm": float(indices.r20mm),
        "r50mm": float(indices.r50mm),
        "r95ptot_mm": indices.r95ptot_mm,
        "r95ptot_frac": indices.r95ptot_frac,
    }


def _nan_para_none(valor: float) -> float | None:
    """``NaN`` vira ``None`` antes da persistência (coluna REAL nullable)."""
    return None if math.isnan(valor) else float(valor)
