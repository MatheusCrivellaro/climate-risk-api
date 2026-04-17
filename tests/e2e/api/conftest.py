"""Fixtures para testes e2e da API.

Cada teste recebe um FastAPI com:

- Banco SQLite in-memory com o schema criado a partir de ``Base.metadata``.
- :func:`get_sessao` substituído por uma dependência que devolve sessões
  ligadas ao engine in-memory.

Isso mantém os testes hermeticos — nenhum arquivo ``.db`` é gerado no
filesystem e não há dependência de migração real do Alembic.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

# Importa os modelos para popular Base.metadata.
import climate_risk.infrastructure.db.modelos  # noqa: F401
from climate_risk.infrastructure.db.base import Base
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.interfaces.app import create_app


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
async def cliente_api(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    app = create_app()

    async def _get_sessao_teste() -> AsyncGenerator[AsyncSession, None]:
        async with async_sessionmaker_() as sessao:
            yield sessao

    app.dependency_overrides[get_sessao] = _get_sessao_teste

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as cliente:
        yield cliente
