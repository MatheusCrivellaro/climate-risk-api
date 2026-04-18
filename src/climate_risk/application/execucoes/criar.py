"""Caso de uso :class:`CriarExecucaoCordex` (UC-02 — lado síncrono).

Cria uma :class:`Execucao` ``pending`` e enfileira um :class:`Job` do tipo
``"processar_cordex"``. Executa no contexto de uma requisição HTTP; o
processamento pesado fica a cargo do Worker (ver
:class:`climate_risk.application.execucoes.processar_cenario.ProcessarCenarioCordex`).

ADR-005: imports restritos a stdlib e :mod:`domain`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.domain.portas.fila_jobs import FilaJobs
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = [
    "CriarExecucaoCordex",
    "ParametrosCriacaoExecucao",
    "ResultadoCriacaoExecucao",
]


@dataclass(frozen=True)
class ParametrosCriacaoExecucao:
    """Entrada agregada do caso de uso.

    Atributos:
        arquivo_nc: Caminho local do arquivo ``.nc`` (validado contra o
            filesystem antes de criar a execução).
        cenario: Rótulo do cenário CORDEX (ex.: ``"rcp45"``).
        variavel: Variável climática (MVP: ``"pr"``).
        bbox: Recorte espacial opcional; ``None`` processa a grade inteira.
        parametros_indices: Parâmetros dos índices anuais.
        p95_baseline: Intervalo fechado de anos para o P95; ``None``
            desativa o cálculo do P95.
        p95_wet_thr: Limiar de "dia chuvoso" em mm/dia para o P95.
    """

    arquivo_nc: str
    cenario: str
    variavel: str
    bbox: BoundingBox | None
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float


@dataclass(frozen=True)
class ResultadoCriacaoExecucao:
    """Retorno do caso de uso, já pronto para o response HTTP."""

    execucao_id: str
    job_id: str
    status: str
    criado_em: datetime


class CriarExecucaoCordex:
    """Cria uma execução CORDEX e enfileira o job correspondente.

    Passos (todos síncronos em relação ao worker):

    1. Valida existência do ``arquivo_nc`` no filesystem (raise
       :class:`ErroArquivoNCNaoEncontrado` em caso negativo).
    2. Cria :class:`Execucao` com ``status=pending`` e ``tipo='grade_bbox'``.
    3. Persiste via :class:`RepositorioExecucoes`.
    4. Enfileira um :class:`Job` ``processar_cordex`` carregando o
       ``execucao_id`` e os parâmetros serializáveis.
    5. Faz upsert da execução para gravar o ``job_id``.

    Observação: a persistência é idempotente por ``id`` (``INSERT ... ON
    CONFLICT DO UPDATE``), logo o segundo ``salvar`` atualiza apenas
    ``job_id``.
    """

    def __init__(
        self,
        repositorio_execucoes: RepositorioExecucoes,
        fila_jobs: FilaJobs,
    ) -> None:
        self._repo = repositorio_execucoes
        self._fila = fila_jobs

    async def executar(self, params: ParametrosCriacaoExecucao) -> ResultadoCriacaoExecucao:
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
            tipo="grade_bbox",
            parametros=_serializar_parametros(params),
            status=StatusExecucao.PENDING,
            criado_em=agora,
            concluido_em=None,
            job_id=None,
        )
        await self._repo.salvar(execucao)

        payload = _montar_payload_job(execucao_id, params)
        job = await self._fila.enfileirar(tipo="processar_cordex", payload=payload)

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

        return ResultadoCriacaoExecucao(
            execucao_id=execucao.id,
            job_id=job.id,
            status=execucao.status,
            criado_em=agora,
        )


def _serializar_parametros(params: ParametrosCriacaoExecucao) -> dict[str, Any]:
    """Converte os parâmetros de domínio para um dict JSON-serializável."""
    return {
        "freq_thr_mm": params.parametros_indices.freq_thr_mm,
        "heavy_thresholds": list(params.parametros_indices.heavy_thresholds),
        "p95_wet_thr": params.p95_wet_thr,
        "p95_baseline": _serializar_baseline(params.p95_baseline),
        "bbox": _serializar_bbox(params.bbox),
    }


def _montar_payload_job(execucao_id: str, params: ParametrosCriacaoExecucao) -> dict[str, Any]:
    """Payload do :class:`Job` — somente tipos JSON."""
    return {
        "execucao_id": execucao_id,
        "arquivo_nc": params.arquivo_nc,
        "cenario": params.cenario,
        "variavel": params.variavel,
        "bbox": _serializar_bbox(params.bbox),
        "parametros_indices": {
            "freq_thr_mm": params.parametros_indices.freq_thr_mm,
            "heavy_thresholds": list(params.parametros_indices.heavy_thresholds),
        },
        "p95_baseline": _serializar_baseline(params.p95_baseline),
        "p95_wet_thr": params.p95_wet_thr,
    }


def _serializar_bbox(bbox: BoundingBox | None) -> dict[str, float] | None:
    if bbox is None:
        return None
    return {
        "lat_min": bbox.lat_min,
        "lat_max": bbox.lat_max,
        "lon_min": bbox.lon_min,
        "lon_max": bbox.lon_max,
    }


def _serializar_baseline(baseline: PeriodoBaseline | None) -> dict[str, int] | None:
    if baseline is None:
        return None
    return {"inicio": baseline.inicio, "fim": baseline.fim}
