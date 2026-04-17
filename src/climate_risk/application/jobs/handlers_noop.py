"""Handler de teste ``noop`` — sem lógica de negócio.

Usado na Slice 5 para validar o fluxo da fila e do worker ponta a ponta:

- Lê ``duracao_segundos`` do payload (default ``0.1``) e faz ``asyncio.sleep``.
- Se ``payload["falhar"] == True``, levanta ``RuntimeError`` com
  ``payload["mensagem_erro"]``.

Mantido permanentemente como utilitário de diagnóstico/smoke-test; não há
regra de negócio aqui e portanto nenhum import de ``domain``.
"""

from __future__ import annotations

import asyncio
from typing import Any


async def handler_noop(payload: dict[str, Any]) -> None:
    """Handler ``noop``: dorme ``duracao_segundos`` e (opcionalmente) falha."""
    duracao = float(payload.get("duracao_segundos", 0.1))
    if duracao > 0:
        await asyncio.sleep(duracao)
    if payload.get("falhar", False):
        mensagem = str(payload.get("mensagem_erro", "Falha simulada"))
        raise RuntimeError(mensagem)
