"""Fixtures para testes de integração da fila SQLite.

Usamos SQLite em **arquivo temporário** (e não ``:memory:``) para:

- Permitir múltiplas sessões concorrentes sobre o mesmo banco físico
  (necessário para o teste de atomicidade).
- Exercitar o caminho de PRAGMAs (``journal_mode=WAL``), que é o modo
  real em produção.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import climate_risk.infrastructure.db.modelos  # noqa: F401 — popula Base.metadata
from climate_risk.infrastructure.db.base import Base
from climate_risk.infrastructure.db.engine import criar_engine, criar_sessionmaker


@pytest_asyncio.fixture
async def fila_engine(tmp_path: Path) -> AsyncGenerator[AsyncEngine, None]:
    """Engine SQLite em arquivo temporário, com WAL ligado pelos PRAGMAs."""
    db_path = tmp_path / "fila_teste.db"
    url = f"sqlite+aiosqlite:///{db_path}"
    engine = criar_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def fila_sessionmaker(
    fila_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return criar_sessionmaker(fila_engine)


@pytest_asyncio.fixture
async def fila_sessao(
    fila_sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    async with fila_sessionmaker() as sessao:
        yield sessao
