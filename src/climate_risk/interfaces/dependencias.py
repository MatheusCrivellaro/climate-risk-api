"""Dependências compartilhadas do FastAPI.

No Slice 0 ainda não há dependências ligadas a banco/fila. Este arquivo
serve como ponto de entrada estável: novos ``Depends`` entram aqui nos
próximos slices conforme os adaptadores de infraestrutura forem criados.
"""

from __future__ import annotations

from climate_risk.core.config import Settings, get_settings


def obter_settings() -> Settings:
    """Adapter para ``FastAPI.Depends`` que devolve a configuração global."""
    return get_settings()
