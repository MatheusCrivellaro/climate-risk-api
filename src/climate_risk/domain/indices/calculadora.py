"""Cálculo de índices anuais de precipitação sobre séries diárias.

Portado de ``legacy/gera_pontos_fornecedores.py`` (função
``annual_indices_for_series``) conforme ADR-001. Paridade bit-a-bit
preservada — inclusive o tratamento de NaN por zero-fill e o uso de
``np.float32`` internamente.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class IndicesAnuais:
    """Índices anuais calculados para uma série diária de precipitação."""

    wet_days: int
    sdii: float
    rx1day: float
    rx5day: float
    r20mm: int
    r50mm: int
    r95ptot_mm: float
    r95ptot_frac: float


@dataclass(frozen=True)
class ParametrosIndices:
    """Parâmetros configuráveis dos índices anuais."""

    freq_thr_mm: float
    heavy_thresholds: tuple[float, float]


def calcular_indices_anuais(
    series: np.ndarray,
    parametros: ParametrosIndices,
    p95_thr: float | None = None,
) -> IndicesAnuais:
    """Calcula :class:`IndicesAnuais` para uma série diária em mm/dia.

    Pré-condições (documentação apenas, não validado em tempo de execução
    para preservar paridade com o legado):

    - ``series`` representa os dias de um único ano.
    - ``series`` já está em mm/dia.
    - ``parametros.freq_thr_mm >= 0``.
    - ``parametros.heavy_thresholds[0] <= parametros.heavy_thresholds[1]``.

    Definições (conforme legado):

    - ``wet_days``: número de dias com ``pr >= freq_thr_mm``.
    - ``sdii``: média de ``pr`` apenas nos dias com ``pr >= freq_thr_mm``.
    - ``rx1day``: máxima precipitação diária do ano.
    - ``rx5day``: máxima soma em janela móvel de 5 dias.
    - ``r20mm`` / ``r50mm``: contagem de dias acima de cada
      ``heavy_threshold``.
    - ``r95ptot_mm`` / ``r95ptot_frac``: soma (e fração) dos dias com
      ``pr > p95_thr``; ``NaN`` quando ``p95_thr is None``.
    """
    arr = np.asarray(series, dtype=np.float32)
    valid = np.isfinite(arr)
    if not np.any(valid):
        return IndicesAnuais(
            wet_days=0,
            sdii=float("nan"),
            rx1day=float("nan"),
            rx5day=float("nan"),
            r20mm=0,
            r50mm=0,
            r95ptot_mm=float("nan"),
            r95ptot_frac=float("nan"),
        )

    x = arr.copy()
    x[~valid] = 0.0

    wet_mask = x >= parametros.freq_thr_mm
    wet_days = int(wet_mask.sum())
    sdii = float(x[wet_mask].mean()) if wet_days > 0 else float("nan")

    rx1day = float(x.max()) if x.size > 0 else float("nan")

    if x.size >= 5:
        kernel = np.ones(5, dtype=np.float32)
        acc5 = np.convolve(x, kernel, mode="valid")
        rx5day = float(acc5.max())
    else:
        rx5day = float("nan")

    heavy_low, heavy_high = parametros.heavy_thresholds
    r20mm = int((x >= heavy_low).sum())
    r50mm = int((x >= heavy_high).sum())

    if p95_thr is not None:
        heavy = x > p95_thr
        r95ptot_mm = float(x[heavy].sum())
        tot = float(x.sum())
        r95ptot_frac = (r95ptot_mm / tot) if tot > 0 else float("nan")
    else:
        r95ptot_mm = float("nan")
        r95ptot_frac = float("nan")

    return IndicesAnuais(
        wet_days=wet_days,
        sdii=sdii,
        rx1day=rx1day,
        rx5day=rx5day,
        r20mm=r20mm,
        r50mm=r50mm,
        r95ptot_mm=r95ptot_mm,
        r95ptot_frac=r95ptot_frac,
    )
