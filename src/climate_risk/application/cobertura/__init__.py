"""Casos de uso de cobertura de fornecedores (Slice 9)."""

from climate_risk.application.cobertura.cobertura_fornecedores import (
    AnalisarCoberturaFornecedores,
    FornecedorCobertura,
    FornecedorEntrada,
    ResultadoCobertura,
)
from climate_risk.application.cobertura.parser_legacy import parsear_lista_legacy

__all__ = [
    "AnalisarCoberturaFornecedores",
    "FornecedorCobertura",
    "FornecedorEntrada",
    "ResultadoCobertura",
    "parsear_lista_legacy",
]
