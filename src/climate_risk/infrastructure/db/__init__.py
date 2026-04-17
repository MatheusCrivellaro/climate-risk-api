"""Camada de persistência (SQLAlchemy async).

ORM models NÃO devem vazar para fora deste pacote — repositórios fazem a
tradução entre modelos ORM e dataclasses de domínio.
"""

from climate_risk.infrastructure.db.base import Base

__all__ = ["Base"]
