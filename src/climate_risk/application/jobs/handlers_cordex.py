"""Handlers de job específicos do fluxo CORDEX (Slice 6).

Exporta :func:`criar_handler_processar_cordex`, uma **fábrica** que devolve
o handler registrado no worker. A fábrica recebe todas as dependências
(repositório de execuções/resultados, leitor NetCDF) e devolve uma
closure que satisfaz o tipo ``Handler`` esperado pelo
:class:`~climate_risk.infrastructure.fila.worker.Worker`.

Razão do padrão factory + closure: o worker não tem conhecimento dos
casos de uso de ``application``. Quem monta o worker (CLI) injeta as
dependências aqui; o handler fica responsável apenas por deserializar
o payload e chamar :class:`ProcessarCenarioCordex`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from climate_risk.application.execucoes.processar_cenario import (
    ParametrosProcessamento,
    ProcessarCenarioCordex,
)
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline

__all__ = ["HandlerProcessarCordex", "criar_handler_processar_cordex"]

HandlerProcessarCordex = Callable[[dict[str, Any]], Awaitable[None]]


def criar_handler_processar_cordex(
    caso_uso: ProcessarCenarioCordex,
) -> HandlerProcessarCordex:
    """Fábrica do handler ``processar_cordex``.

    Args:
        caso_uso: Instância de :class:`ProcessarCenarioCordex` já com todas
            as dependências injetadas (leitor NetCDF, repositórios).

    Returns:
        Coroutine ``(payload) -> None`` pronta para o ``Worker``.
    """

    async def _handler(payload: dict[str, Any]) -> None:
        params = _deserializar_payload(payload)
        await caso_uso.executar(params)

    return _handler


def _deserializar_payload(payload: dict[str, Any]) -> ParametrosProcessamento:
    """Converte o payload JSON do Job em :class:`ParametrosProcessamento`."""
    bbox = _ler_bbox(payload.get("bbox"))
    baseline = _ler_baseline(payload.get("p95_baseline"))
    parametros_indices_raw = payload["parametros_indices"]
    heavy = tuple(parametros_indices_raw["heavy_thresholds"])
    parametros_indices = ParametrosIndices(
        freq_thr_mm=float(parametros_indices_raw["freq_thr_mm"]),
        heavy_thresholds=(float(heavy[0]), float(heavy[1])),
    )
    return ParametrosProcessamento(
        execucao_id=str(payload["execucao_id"]),
        arquivo_nc=str(payload["arquivo_nc"]),
        variavel=str(payload["variavel"]),
        bbox=bbox,
        parametros_indices=parametros_indices,
        p95_baseline=baseline,
        p95_wet_thr=float(payload["p95_wet_thr"]),
    )


def _ler_bbox(raw: dict[str, Any] | None) -> BoundingBox | None:
    if raw is None:
        return None
    return BoundingBox(
        lat_min=float(raw["lat_min"]),
        lat_max=float(raw["lat_max"]),
        lon_min=float(raw["lon_min"]),
        lon_max=float(raw["lon_max"]),
    )


def _ler_baseline(raw: dict[str, Any] | None) -> PeriodoBaseline | None:
    if raw is None:
        return None
    return PeriodoBaseline(inicio=int(raw["inicio"]), fim=int(raw["fim"]))
