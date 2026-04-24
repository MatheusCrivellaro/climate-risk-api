"""Slice 15: cria tabela ``resultado_estresse_hidrico`` (formato wide).

Revision ID: 3e5b1c0d4f22
Revises: 7a8c2d4f9e13
Create Date: 2026-04-24

O pipeline de estresse hídrico (Slices 13–15) persiste seus resultados
numa tabela independente de ``resultado_indice``. O formato é **wide**:
frequência (``dias secos quentes``) e intensidade (``mm``) na mesma linha,
indexadas por ``(execucao_id, municipio_id, ano)``.

Motivação:

- Frequência e intensidade "andam juntas" no consumo downstream.
- Consultas agregadas (ex.: médias ponderadas) ficam triviais em wide.
- Mantém o pipeline de precipitação extrema inalterado.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3e5b1c0d4f22"
down_revision: str | Sequence[str] | None = "7a8c2d4f9e13"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "resultado_estresse_hidrico",
        sa.Column("id", sa.String(length=40), nullable=False),
        sa.Column("execucao_id", sa.String(length=40), nullable=False),
        sa.Column("municipio_id", sa.Integer(), nullable=False),
        sa.Column("ano", sa.Integer(), nullable=False),
        sa.Column("cenario", sa.String(length=16), nullable=False),
        sa.Column("frequencia_dias_secos_quentes", sa.Integer(), nullable=False),
        sa.Column("intensidade_mm", sa.Float(), nullable=False),
        sa.Column("criado_em", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["execucao_id"],
            ["execucao.id"],
            name=op.f("fk_resultado_estresse_hidrico_execucao_id_execucao"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_resultado_estresse_hidrico")),
        sa.UniqueConstraint(
            "execucao_id",
            "municipio_id",
            "ano",
            name="uq_resultado_estresse_hidrico_execucao_mun_ano",
        ),
    )
    with op.batch_alter_table("resultado_estresse_hidrico", schema=None) as batch_op:
        batch_op.create_index(
            op.f("ix_resultado_estresse_hidrico_execucao_id"),
            ["execucao_id"],
            unique=False,
        )
        batch_op.create_index(
            op.f("ix_resultado_estresse_hidrico_municipio_id"),
            ["municipio_id"],
            unique=False,
        )
        batch_op.create_index(
            op.f("ix_resultado_estresse_hidrico_ano"),
            ["ano"],
            unique=False,
        )
        batch_op.create_index(
            op.f("ix_resultado_estresse_hidrico_cenario"),
            ["cenario"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("resultado_estresse_hidrico", schema=None) as batch_op:
        batch_op.drop_index(op.f("ix_resultado_estresse_hidrico_cenario"))
        batch_op.drop_index(op.f("ix_resultado_estresse_hidrico_ano"))
        batch_op.drop_index(op.f("ix_resultado_estresse_hidrico_municipio_id"))
        batch_op.drop_index(op.f("ix_resultado_estresse_hidrico_execucao_id"))
    op.drop_table("resultado_estresse_hidrico")
