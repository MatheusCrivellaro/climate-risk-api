"""Rota de liveness (``GET /health``)."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["admin"])


@router.get("/health", summary="Verificação de liveness.")
async def health() -> dict[str, str]:
    """Retorna ``{"status": "ok"}`` quando o processo responde."""
    return {"status": "ok"}
