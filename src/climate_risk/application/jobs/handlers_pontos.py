"""Handler do job-type ``calcular_pontos`` (Slice 7).

Espelha o padrão de :mod:`climate_risk.application.jobs.handlers_cordex`:
uma fábrica recebe um caso de uso totalmente injetado e devolve uma
closure compatível com o tipo ``Handler`` do
:class:`~climate_risk.infrastructure.fila.worker.Worker`.

Responsabilidade do handler: deserializar o payload JSON em
:class:`ParametrosProcessamentoPontos` e invocar :class:`ProcessarPontosLote`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from climate_risk.application.calculos.calcular_por_pontos import PontoEntradaDominio
from climate_risk.application.calculos.processar_pontos_lote import (
    ParametrosProcessamentoPontos,
    ProcessarPontosLote,
)
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline

__all__ = ["HandlerCalcularPontos", "criar_handler_calcular_pontos"]

HandlerCalcularPontos = Callable[[dict[str, Any]], Awaitable[None]]


def criar_handler_calcular_pontos(
    caso_uso: ProcessarPontosLote,
) -> HandlerCalcularPontos:
    """Fábrica do handler ``calcular_pontos``.

    Args:
        caso_uso: :class:`ProcessarPontosLote` já com todas as
            dependências injetadas (leitor NetCDF, repositórios).

    Returns:
        Coroutine ``(payload) -> None`` pronta para o :class:`Worker`.
    """

    async def _handler(payload: dict[str, Any]) -> None:
        params = _deserializar_payload(payload)
        await caso_uso.executar(params)

    return _handler


def _deserializar_payload(payload: dict[str, Any]) -> ParametrosProcessamentoPontos:
    """Converte o payload JSON do Job em :class:`ParametrosProcessamentoPontos`."""
    parametros_indices_raw = payload["parametros_indices"]
    heavy = tuple(parametros_indices_raw["heavy_thresholds"])
    parametros_indices = ParametrosIndices(
        freq_thr_mm=float(parametros_indices_raw["freq_thr_mm"]),
        heavy_thresholds=(float(heavy[0]), float(heavy[1])),
    )
    pontos = [_ler_ponto(p) for p in payload["pontos"]]
    return ParametrosProcessamentoPontos(
        execucao_id=str(payload["execucao_id"]),
        arquivo_nc=str(payload["arquivo_nc"]),
        cenario=str(payload["cenario"]),
        variavel=str(payload["variavel"]),
        pontos=pontos,
        parametros_indices=parametros_indices,
        p95_baseline=_ler_baseline(payload.get("p95_baseline")),
        p95_wet_thr=float(payload["p95_wet_thr"]),
    )


def _ler_ponto(raw: dict[str, Any]) -> PontoEntradaDominio:
    identificador_raw = raw.get("identificador")
    identificador = None if identificador_raw is None else str(identificador_raw)
    return PontoEntradaDominio(
        lat=float(raw["lat"]),
        lon=float(raw["lon"]),
        identificador=identificador,
    )


def _ler_baseline(raw: dict[str, Any] | None) -> PeriodoBaseline | None:
    if raw is None:
        return None
    return PeriodoBaseline(inicio=int(raw["inicio"]), fim=int(raw["fim"]))
