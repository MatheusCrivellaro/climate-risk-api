"""Slice 10: amplia ``fornecedor`` com ``cidade``, ``uf``, ``atualizado_em``.

Revision ID: 7a8c2d4f9e13
Revises: 69aa6a0f411b
Create Date: 2026-04-19

O CRUD do Slice 10 precisa de cidade/UF declarados pelo cliente (antes de
qualquer geocodificação). ``lat`` e ``lon`` ficam opcionais — o import em
lote não exige coordenadas no payload, que podem ser preenchidas depois
via ``POST /localizacoes/geocodificar``.

Os dois índices compostos suportam:
- Listagem filtrada por UF/cidade (``GET /fornecedores?uf=SP&cidade=Campinas``).
- Detecção de duplicatas em import (``nome+cidade+uf``).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7a8c2d4f9e13"
down_revision: str | Sequence[str] | None = "69aa6a0f411b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("fornecedor", schema=None) as batch_op:
        # Bases populadas antes do Slice 10 ganham valores sentinelas —
        # novos registros sempre chegam com valor explícito do aplicativo.
        batch_op.add_column(
            sa.Column(
                "cidade",
                sa.String(length=120),
                nullable=False,
                server_default="",
            )
        )
        batch_op.add_column(
            sa.Column(
                "uf",
                sa.String(length=2),
                nullable=False,
                server_default="",
            )
        )
        batch_op.add_column(
            sa.Column(
                "atualizado_em",
                sa.String(length=32),
                nullable=False,
                server_default="1970-01-01T00:00:00+00:00",
            )
        )
        batch_op.alter_column("lat", existing_type=sa.Float(), nullable=True)
        batch_op.alter_column("lon", existing_type=sa.Float(), nullable=True)
        batch_op.create_index(
            "idx_fornecedor_uf_cidade",
            ["uf", "cidade"],
            unique=False,
        )
        batch_op.create_index(
            "idx_fornecedor_nome_cidade_uf",
            ["nome", "cidade", "uf"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("fornecedor", schema=None) as batch_op:
        batch_op.drop_index("idx_fornecedor_nome_cidade_uf")
        batch_op.drop_index("idx_fornecedor_uf_cidade")
        batch_op.alter_column("lon", existing_type=sa.Float(), nullable=False)
        batch_op.alter_column("lat", existing_type=sa.Float(), nullable=False)
        batch_op.drop_column("atualizado_em")
        batch_op.drop_column("uf")
        batch_op.drop_column("cidade")
