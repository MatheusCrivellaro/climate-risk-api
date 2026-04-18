"""Caso de uso :class:`CriarExecucaoPorPontos` (UC-03 — lado síncrono do fluxo async).

Para lotes com mais de ``sincrono_pontos_max`` pontos, a rota
``POST /calculos/pontos`` não executa o cálculo de forma síncrona. Em vez
disso, delega a este caso de uso, que cria uma :class:`Execucao` em
``pending`` e enfileira um :class:`Job` do tipo ``"calcular_pontos"``
para ser consumido pelo Worker.

Padrão espelhado de :class:`CriarExecucaoCordex` (Slice 6). A única
diferença relevante é o payload do job, que carrega a lista de pontos
serializada como ``list[dict]`` em vez de um bbox.

ADR-005: apenas stdlib e ``domain``/``application`` são importados.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from climate_risk.application.calculos.calcular_por_pontos import PontoEntradaDominio
from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.domain.portas.fila_jobs import FilaJobs
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = [
    "CriarExecucaoPorPontos",
    "ParametrosCriacaoExecucaoPontos",
    "ResultadoCriacaoExecucaoPontos",
]


@dataclass(frozen=True)
class ParametrosCriacaoExecucaoPontos:
    """Entrada do caso de uso.

    Atributos:
        arquivo_nc: Caminho local do ``.nc`` validado contra o filesystem.
        cenario: Rótulo do cenário (ex.: ``"rcp45"``).
        variavel: Variável climática (MVP: ``"pr"``).
        pontos: Lista de pontos a avaliar. A rota HTTP já validou que
            o tamanho excede o limite síncrono.
        parametros_indices: Parâmetros dos índices anuais.
        p95_baseline: Intervalo fechado de anos para o P95; ``None``
            desativa o cálculo.
        p95_wet_thr: Limiar de "dia chuvoso" em mm/dia.
    """

    arquivo_nc: str
    cenario: str
    variavel: str
    pontos: Sequence[PontoEntradaDominio]
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float


@dataclass(frozen=True)
class ResultadoCriacaoExecucaoPontos:
    """Retorno do caso de uso, já pronto para o response HTTP ``202``."""

    execucao_id: str
    job_id: str
    status: str
    total_pontos: int
    criado_em: datetime


class CriarExecucaoPorPontos:
    """Cria uma :class:`Execucao` de lote e enfileira o :class:`Job` correspondente.

    Passos (síncronos em relação ao worker):

    1. Valida existência do ``arquivo_nc`` (levanta
       :class:`ErroArquivoNCNaoEncontrado`).
    2. Cria :class:`Execucao` com ``status=pending`` e ``tipo="pontos_lote"``.
    3. Persiste via :class:`RepositorioExecucoes`.
    4. Enfileira um :class:`Job` ``calcular_pontos`` com payload
       JSON-serializável.
    5. Upsert da execução para gravar o ``job_id``.
    """

    def __init__(
        self,
        repositorio_execucoes: RepositorioExecucoes,
        fila_jobs: FilaJobs,
    ) -> None:
        self._repo = repositorio_execucoes
        self._fila = fila_jobs

    async def executar(
        self, params: ParametrosCriacaoExecucaoPontos
    ) -> ResultadoCriacaoExecucaoPontos:
        """Executa o fluxo; ver docstring da classe."""
        if not Path(params.arquivo_nc).exists():
            raise ErroArquivoNCNaoEncontrado(
                caminho=params.arquivo_nc,
                detalhe="arquivo não existe no filesystem.",
            )

        agora = utc_now()
        execucao_id = gerar_id("exec")
        execucao = Execucao(
            id=execucao_id,
            cenario=params.cenario,
            variavel=params.variavel,
            arquivo_origem=params.arquivo_nc,
            tipo="pontos_lote",
            parametros=_serializar_parametros(params),
            status=StatusExecucao.PENDING,
            criado_em=agora,
            concluido_em=None,
            job_id=None,
        )
        await self._repo.salvar(execucao)

        payload = _montar_payload_job(execucao_id, params)
        job = await self._fila.enfileirar(tipo="calcular_pontos", payload=payload)

        execucao_com_job = Execucao(
            id=execucao.id,
            cenario=execucao.cenario,
            variavel=execucao.variavel,
            arquivo_origem=execucao.arquivo_origem,
            tipo=execucao.tipo,
            parametros=execucao.parametros,
            status=execucao.status,
            criado_em=execucao.criado_em,
            concluido_em=execucao.concluido_em,
            job_id=job.id,
        )
        await self._repo.salvar(execucao_com_job)

        return ResultadoCriacaoExecucaoPontos(
            execucao_id=execucao.id,
            job_id=job.id,
            status=execucao.status,
            total_pontos=len(params.pontos),
            criado_em=agora,
        )


def _serializar_parametros(params: ParametrosCriacaoExecucaoPontos) -> dict[str, Any]:
    """Dict JSON-serializável persistido em ``Execucao.parametros``."""
    return {
        "freq_thr_mm": params.parametros_indices.freq_thr_mm,
        "heavy_thresholds": list(params.parametros_indices.heavy_thresholds),
        "p95_wet_thr": params.p95_wet_thr,
        "p95_baseline": _serializar_baseline(params.p95_baseline),
        "total_pontos": len(params.pontos),
    }


def _montar_payload_job(
    execucao_id: str, params: ParametrosCriacaoExecucaoPontos
) -> dict[str, Any]:
    """Payload do :class:`Job` — somente tipos JSON."""
    return {
        "execucao_id": execucao_id,
        "arquivo_nc": params.arquivo_nc,
        "cenario": params.cenario,
        "variavel": params.variavel,
        "pontos": [_serializar_ponto(p) for p in params.pontos],
        "parametros_indices": {
            "freq_thr_mm": params.parametros_indices.freq_thr_mm,
            "heavy_thresholds": list(params.parametros_indices.heavy_thresholds),
        },
        "p95_baseline": _serializar_baseline(params.p95_baseline),
        "p95_wet_thr": params.p95_wet_thr,
    }


def _serializar_ponto(ponto: PontoEntradaDominio) -> dict[str, Any]:
    return {
        "lat": ponto.lat,
        "lon": ponto.lon,
        "identificador": ponto.identificador,
    }


def _serializar_baseline(baseline: PeriodoBaseline | None) -> dict[str, int] | None:
    if baseline is None:
        return None
    return {"inicio": baseline.inicio, "fim": baseline.fim}
