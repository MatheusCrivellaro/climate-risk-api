"""Conversores de unidade para precipitação.

Portado de ``legacy/gera_pontos_fornecedores.py`` (função
``convert_pr_to_mm_per_day``).

ATENÇÃO — preservação bit-a-bit da heurística ``vmax < 5.0``: ver ADR-007.
A heurística é mantida intencionalmente no MVP para garantir paridade
numérica com a baseline congelada; a correção definitiva (detecção
puramente por metadados) está planejada para o pós-MVP.
"""

from __future__ import annotations

from dataclasses import dataclass

import xarray as xr


@dataclass(frozen=True)
class ResultadoConversao:
    """Resultado da conversão de unidades de precipitação."""

    dados: xr.DataArray
    unidade_original: str
    conversao_aplicada: bool


class ConversorPrecipitacao:
    """Conversor de variáveis de precipitação para mm/dia."""

    @staticmethod
    def para_mm_por_dia(da: xr.DataArray) -> ResultadoConversao:
        """Converte um ``DataArray`` de precipitação para mm/dia.

        Replica EXATAMENTE a lógica do legado:

        - Se ``units`` contém ``kg m-2 s-1``, ``kg m^-2 s^-1``, ``mm s-1``
          ou ``mm/s`` → multiplica por 86400.
        - Ou se ``float(da.max()) < 5.0`` → multiplica por 86400
          (heurística preservada; bug documentado em ADR-007).
        - Caso contrário, mantém os valores.

        A saída carrega ``attrs['units'] = 'mm/day'``.
        """
        unidade_original = str(da.attrs.get("units", "") or "")
        units = unidade_original.lower()
        vmax = float(da.max())

        # Heurística legada preservada bit-a-bit — bug intencional, ver ADR-007.
        deve_converter = (
            ("kg m-2 s-1" in units)
            or ("kg m^-2 s^-1" in units)
            or ("mm s-1" in units)
            or ("mm/s" in units)
            or vmax < 5.0
        )

        if deve_converter:
            da = da * 86400.0

        da.attrs["units"] = "mm/day"
        return ResultadoConversao(
            dados=da,
            unidade_original=unidade_original,
            conversao_aplicada=deve_converter,
        )
