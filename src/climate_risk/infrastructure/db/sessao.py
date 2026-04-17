"""Helpers de sessão para injeção via FastAPI.

``get_sessionmaker`` inicializa a engine e o sessionmaker na primeira chamada
(lazy) a partir de ``core/config.py``. ``get_sessao`` é a dependência FastAPI
que entrega uma sessão por request e garante ``close()`` ao final.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.core.config import get_settings
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker


@lru_cache(maxsize=1)
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Instância única por processo, construída a partir das settings."""
    settings = get_settings()
    engine = criar_engine(settings.database_url)
    return criar_sessionmaker(engine)


async def get_sessao() -> AsyncGenerator[AsyncSession, None]:
    """Dependência FastAPI que entrega uma sessão por request."""
    sessionmaker_ = get_sessionmaker()
    async with sessionmaker_() as sessao:
        yield sessao
