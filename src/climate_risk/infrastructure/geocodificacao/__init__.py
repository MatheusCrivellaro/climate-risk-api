"""Adaptadores concretos para geocodificação IBGE (Slice 8)."""

from climate_risk.infrastructure.geocodificacao.calculador_shapely import CalculadorShapely
from climate_risk.infrastructure.geocodificacao.cliente_ibge_http import ClienteIBGEHttp

__all__ = ["CalculadorShapely", "ClienteIBGEHttp"]
