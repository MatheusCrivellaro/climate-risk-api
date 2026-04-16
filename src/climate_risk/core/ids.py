"""Geração de identificadores únicos prefixados (ULID)."""

from __future__ import annotations

from ulid import ULID


def gerar_id(prefixo: str) -> str:
    """Retorna ``<prefixo>_<ULID>`` em formato canônico."""
    return f"{prefixo}_{ULID()}"
