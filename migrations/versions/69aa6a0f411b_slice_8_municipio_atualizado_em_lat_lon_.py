"""Slice 8: adiciona ``municipio.atualizado_em`` e torna lat/lon opcionais.

Revision ID: 69aa6a0f411b
Revises: 0001_initial
Create Date: 2026-04-19

Permite inserir um município ainda sem centroide (dois passos: catálogo
IBGE e depois malha) e registra o momento da última sincronização.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "69aa6a0f411b"
down_revision: str | Sequence[str] | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("municipio", schema=None) as batch_op:
        # server_default permite upgrade sobre bases já populadas; os novos
        # registros sempre vêm com valor explícito do aplicativo.
        batch_op.add_column(
            sa.Column(
                "atualizado_em",
                sa.String(length=32),
                nullable=False,
                server_default="1970-01-01T00:00:00+00:00",
            )
        )
        batch_op.alter_column("lat_centroide", existing_type=sa.Float(), nullable=True)
        batch_op.alter_column("lon_centroide", existing_type=sa.Float(), nullable=True)


def downgrade() -> None:
    with op.batch_alter_table("municipio", schema=None) as batch_op:
        batch_op.alter_column("lon_centroide", existing_type=sa.Float(), nullable=False)
        batch_op.alter_column("lat_centroide", existing_type=sa.Float(), nullable=False)
        batch_op.drop_column("atualizado_em")
