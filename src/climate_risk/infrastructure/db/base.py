"""Classe ``Base`` declarativa do SQLAlchemy 2.x.

A convenção de nomenclatura de constraints garante que migrations geradas
pelo Alembic tenham nomes determinísticos e, portanto, possam ser revertidas
sem surpresas (importante para portabilidade para PostgreSQL no futuro).
"""

from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
}


class Base(DeclarativeBase):
    """Base declarativa compartilhada por todos os modelos ORM."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)
