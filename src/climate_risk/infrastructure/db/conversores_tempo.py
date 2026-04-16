"""Conversores entre ``datetime`` e ISO 8601 UTC em ``TEXT``.

Centralizamos para evitar divergência entre repositórios. Todos os
timestamps persistidos são em UTC; se a entrada for naive, assumimos UTC
(responsabilidade do chamador garantir).
"""

from __future__ import annotations

from datetime import UTC, datetime


def datetime_para_iso(valor: datetime | None) -> str | None:
    if valor is None:
        return None
    if valor.tzinfo is None:
        valor = valor.replace(tzinfo=UTC)
    return valor.astimezone(UTC).isoformat()


def iso_para_datetime(valor: str | None) -> datetime | None:
    if valor is None:
        return None
    return datetime.fromisoformat(valor)
