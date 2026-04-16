"""Funções utilitárias de tempo."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Retorna ``datetime`` no fuso UTC com offset explícito."""
    return datetime.now(UTC)
