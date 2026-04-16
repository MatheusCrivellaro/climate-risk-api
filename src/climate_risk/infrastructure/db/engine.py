"""Construção de engine e sessionmaker assíncronos.

Para SQLite em arquivo, habilitamos dois PRAGMAs em cada conexão via event
listener:

- ``foreign_keys = ON``: SQLite desabilita FKs por padrão. Sem isso, nossas
  ``ForeignKey`` ficariam apenas decorativas.
- ``journal_mode = WAL``: permite leitores concorrentes com um escritor, o
  que é essencial quando a API (leitura) e o worker (escrita) rodam juntos.

Esses PRAGMAs **não** são aplicados em ``:memory:`` (usado em testes) — o
banco é recriado a cada conexão e não se beneficia deles. Outros dialetos
(Postgres no futuro) também são ignorados.
"""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import ConnectionPoolEntry


def _deve_aplicar_pragmas(url: str) -> bool:
    if not url.startswith("sqlite"):
        return False
    return ":memory:" not in url


def criar_engine(url: str) -> AsyncEngine:
    """Cria uma :class:`AsyncEngine` aplicando PRAGMAs SQLite quando cabível."""
    engine = create_async_engine(url, future=True)

    if _deve_aplicar_pragmas(url):

        @event.listens_for(engine.sync_engine, "connect")
        def _ligar_pragmas(
            dbapi_connection: DBAPIConnection,
            _connection_record: ConnectionPoolEntry,
        ) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.close()

    return engine


def criar_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Retorna ``async_sessionmaker`` configurado (sem autoflush, expira após commit)."""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autoflush=False,
        expire_on_commit=False,
    )
