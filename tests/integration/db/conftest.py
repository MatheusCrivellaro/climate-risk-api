"""Fixtures para testes de integração com o banco.

Usamos SQLite in-memory (`:memory:`) por teste, via fixture que cria o
schema direto de ``Base.metadata`` (equivalente a rodar as migrations, mas
muito mais rápido). Cada teste recebe uma sessão limpa e, ao final, tudo
é descartado.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

# Importa os modelos para popular Base.metadata.
import climate_risk.infrastructure.db.modelos  # noqa: F401
from climate_risk.infrastructure.db.base import Base
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker


@pytest_asyncio.fixture
async def async_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = criar_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def async_sessionmaker_(
    async_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return criar_sessionmaker(async_engine)


@pytest_asyncio.fixture
async def async_session(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    async with async_sessionmaker_() as sessao:
        yield sessao
