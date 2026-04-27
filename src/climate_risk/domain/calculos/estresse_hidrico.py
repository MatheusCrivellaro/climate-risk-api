"""Cálculo de índices anuais de estresse hídrico (Slice 13).

Funções puras sobre arrays ``numpy`` — sem I/O, sem xarray, sem pandas.
Operam sobre séries diárias **já alinhadas** em um eixo temporal comum:
os três arrays de entrada precisam ter o mesmo comprimento e o i-ésimo
elemento de cada um deve corresponder ao mesmo dia.

Regras de NaN (consistentes com a decisão em ADR-009):

- ``NaN`` em qualquer das três séries (pr/tas/evap) no dia ``i`` → o dia
  é descartado **de todos** os cálculos (não conta como seco, não entra
  em soma, não é incluído no denominador de nada).
- ``NaN`` isolado em ``evap`` mas com ``pr``/``tas`` válidos também
  descarta o dia (coerência com o critério anterior — tratar NaN de
  forma independente daria resultados inconsistentes entre os índices).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ParametrosIndicesEstresseHidrico:
    """Limiares configuráveis dos índices de estresse hídrico.

    Atributos:
        limiar_pr_mm_dia: Teto de precipitação para o dia ser "seco".
            Dia seco quando ``pr <= limiar_pr_mm_dia``. Default ``1.0`` mm/dia
            (compatível com a definição ETCCDI de dia seco).
        limiar_tas_c: Piso de temperatura para o dia ser "quente". Dia quente
            quando ``tas >= limiar_tas_c``. Default ``30.0`` °C — reflete a
            definição operacional adotada pelo time de dados para o MVP.
    """

    limiar_pr_mm_dia: float = 1.0
    limiar_tas_c: float = 30.0


@dataclass(frozen=True)
class IndicesAnuaisEstresseHidrico:
    """Resultado anual do cálculo de estresse hídrico.

    Atributos:
        dias_secos_quentes: Contagem (inteiro ≥ 0) de dias onde ``pr`` e
            ``tas`` satisfazem os limiares simultaneamente e nenhuma
            das três variáveis era ``NaN``.
        intensidade_mm_dia: **Média** do déficit (``evap - pr``) por dia
            seco quente do ano, em mm/dia. Calculado como
            ``soma(deficit nos dias secos quentes) / dias_secos_quentes``.
            Se não há dias secos quentes (``dias_secos_quentes == 0``),
            retorna ``0.0`` por convenção (não ``NaN``) — interpretação:
            "não houve estresse no ano". Definição introduzida na Slice 19;
            a definição anterior (soma total em mm) foi descartada — ver
            ADR-011.
        deficit_total_mm: Soma do déficit diário acumulado em **todos** os
            dias válidos do ano (inclusive dias chuvosos, onde o déficit
            pode ser negativo). Em mm.
    """

    dias_secos_quentes: int
    intensidade_mm_dia: float
    deficit_total_mm: float


def calcular_dias_secos_quentes(
    pr_mm_dia: np.ndarray,
    tas_c: np.ndarray,
    params: ParametrosIndicesEstresseHidrico,
) -> np.ndarray:
    """Máscara booleana: ``True`` nos dias secos quentes.

    Não considera ``NaN`` — callers que precisem dessa lógica devem aplicar
    `np.isfinite` antes. Mantivemos a função "pura booleana" para poder
    compor com outras máscaras (ex.: "seco quente E no verão") sem lidar
    com ``nan == nan → False`` já embutido.
    """
    return (pr_mm_dia <= params.limiar_pr_mm_dia) & (tas_c >= params.limiar_tas_c)


def calcular_deficit_hidrico_diario(
    evap_mm_dia: np.ndarray,
    pr_mm_dia: np.ndarray,
) -> np.ndarray:
    """Déficit hídrico diário = ``evap - pr``.

    Positivo quando a evaporação excede a precipitação (dia "seco" em
    termos hídricos, independente do limiar de chuva). Negativo em dias
    chuvosos onde entra mais água do que sai.
    """
    return np.asarray(evap_mm_dia - pr_mm_dia)


def calcular_indices_anuais_estresse_hidrico(
    pr_mm_dia: np.ndarray,
    tas_c: np.ndarray,
    evap_mm_dia: np.ndarray,
    params: ParametrosIndicesEstresseHidrico,
) -> IndicesAnuaisEstresseHidrico:
    """Computa os três índices de estresse hídrico para um ano.

    Pré-condições (não validadas em tempo de execução por performance):

    - As três séries têm o mesmo comprimento.
    - ``pr``/``evap`` estão em ``mm/dia``; ``tas`` em ``°C``.
    - Cada série corresponde a dias sucessivos de um **único** ano
      (o caller separa por ano antes).
    """
    pr = np.asarray(pr_mm_dia, dtype=np.float64)
    tas = np.asarray(tas_c, dtype=np.float64)
    evap = np.asarray(evap_mm_dia, dtype=np.float64)

    validos = np.isfinite(pr) & np.isfinite(tas) & np.isfinite(evap)
    if not np.any(validos):
        return IndicesAnuaisEstresseHidrico(
            dias_secos_quentes=0,
            intensidade_mm_dia=0.0,
            deficit_total_mm=0.0,
        )

    pr_v = pr[validos]
    tas_v = tas[validos]
    evap_v = evap[validos]

    mascara_secos_quentes = calcular_dias_secos_quentes(pr_v, tas_v, params)
    deficit_diario = calcular_deficit_hidrico_diario(evap_v, pr_v)

    dias_secos_quentes = int(mascara_secos_quentes.sum())
    if dias_secos_quentes > 0:
        soma_deficit = float(deficit_diario[mascara_secos_quentes].sum())
        intensidade_mm_dia = soma_deficit / dias_secos_quentes
    else:
        intensidade_mm_dia = 0.0
    deficit_total_mm = float(deficit_diario.sum())

    return IndicesAnuaisEstresseHidrico(
        dias_secos_quentes=dias_secos_quentes,
        intensidade_mm_dia=intensidade_mm_dia,
        deficit_total_mm=deficit_total_mm,
    )
