"""DTO de saída dos leitores de CSV/XLSX (Slice 10).

Mantemos um dataclass local à camada de infraestrutura para não criar um
import reverso (infrastructure → application). A camada ``interfaces``
converte :class:`LinhaImportacaoBruta` para
:class:`climate_risk.application.fornecedores.LinhaImportacao` antes de
invocar o caso de uso.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LinhaImportacaoBruta:
    """Uma linha crua lida do arquivo, com o número da linha na origem."""

    nome: str
    cidade: str
    uf: str
    numero_linha: int
