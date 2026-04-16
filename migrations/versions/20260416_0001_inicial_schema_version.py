"""Migration inicial: tabela de controle ``schema_version``.

Revision ID: 0001_inicial
Revises:
Create Date: 2026-04-16

Existe apenas para garantir que o pipeline Alembic está operante. Tabelas
reais (municipios, execucoes, resultados, jobs, etc.) entram a partir do
Slice 2.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_inicial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "schema_version",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "aplicado_em",
            sa.Text(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_table("schema_version")
