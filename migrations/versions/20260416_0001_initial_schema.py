"""Schema inicial: municipio, fornecedor, execucao, resultado_indice, job.

Revision ID: 0001_initial
Revises:
Create Date: 2026-04-16

Cria o esquema completo do MVP conforme ``docs/desenho-api.md`` seção 5.
Disciplina de portabilidade (ADR-003):

- Timestamps em ``String(32)`` ISO 8601 UTC.
- Campos JSON (``parametros``, ``payload``) em ``Text`` com serialização
  explícita.
- IDs em aplicação (``core/ids.py``), exceto ``municipio.id`` (IBGE).
- ``CHECK constraint`` para enums de status.
- Índice parcial ``idx_job_pending_fila`` (SQLite e PostgreSQL).
- Convenção de nomenclatura de constraints herdada de ``Base.metadata``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "job",
        sa.Column("id", sa.String(length=40), nullable=False),
        sa.Column("tipo", sa.String(length=60), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("tentativas", sa.Integer(), nullable=False),
        sa.Column("max_tentativas", sa.Integer(), nullable=False),
        sa.Column("criado_em", sa.String(length=32), nullable=False),
        sa.Column("iniciado_em", sa.String(length=32), nullable=True),
        sa.Column("concluido_em", sa.String(length=32), nullable=True),
        sa.Column("heartbeat", sa.String(length=32), nullable=True),
        sa.Column("erro", sa.Text(), nullable=True),
        sa.Column("proxima_tentativa_em", sa.String(length=32), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','running','completed','failed','canceled')",
            name=op.f("ck_job_status_valido"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job")),
    )
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.create_index(
            "idx_job_pending_fila",
            ["status", "proxima_tentativa_em"],
            unique=False,
            sqlite_where=sa.text("status = 'pending'"),
            postgresql_where=sa.text("status = 'pending'"),
        )

    op.create_table(
        "municipio",
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("nome", sa.String(length=120), nullable=False),
        sa.Column("nome_normalizado", sa.String(length=120), nullable=False),
        sa.Column("uf", sa.String(length=2), nullable=False),
        sa.Column("lat_centroide", sa.Float(), nullable=False),
        sa.Column("lon_centroide", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_municipio")),
    )
    with op.batch_alter_table("municipio", schema=None) as batch_op:
        batch_op.create_index(
            "idx_municipio_uf_nome_normalizado",
            ["uf", "nome_normalizado"],
            unique=False,
        )

    op.create_table(
        "execucao",
        sa.Column("id", sa.String(length=40), nullable=False),
        sa.Column("cenario", sa.String(length=40), nullable=False),
        sa.Column("variavel", sa.String(length=20), nullable=False),
        sa.Column("arquivo_origem", sa.Text(), nullable=False),
        sa.Column("tipo", sa.String(length=20), nullable=False),
        sa.Column("parametros", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("criado_em", sa.String(length=32), nullable=False),
        sa.Column("concluido_em", sa.String(length=32), nullable=True),
        sa.Column("job_id", sa.String(length=40), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','running','completed','failed','canceled')",
            name=op.f("ck_execucao_status_valido"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name=op.f("fk_execucao_job_id_job"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execucao")),
    )

    op.create_table(
        "fornecedor",
        sa.Column("id", sa.String(length=40), nullable=False),
        sa.Column("identificador_externo", sa.String(length=120), nullable=True),
        sa.Column("nome", sa.String(length=200), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("municipio_id", sa.Integer(), nullable=True),
        sa.Column("criado_em", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["municipio_id"],
            ["municipio.id"],
            name=op.f("fk_fornecedor_municipio_id_municipio"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fornecedor")),
    )

    op.create_table(
        "resultado_indice",
        sa.Column("id", sa.String(length=40), nullable=False),
        sa.Column("execucao_id", sa.String(length=40), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("lat_input", sa.Float(), nullable=True),
        sa.Column("lon_input", sa.Float(), nullable=True),
        sa.Column("ano", sa.Integer(), nullable=False),
        sa.Column("nome_indice", sa.String(length=40), nullable=False),
        sa.Column("valor", sa.Float(), nullable=True),
        sa.Column("unidade", sa.String(length=20), nullable=False),
        sa.Column("municipio_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["execucao_id"],
            ["execucao.id"],
            name=op.f("fk_resultado_indice_execucao_id_execucao"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["municipio_id"],
            ["municipio.id"],
            name=op.f("fk_resultado_indice_municipio_id_municipio"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_resultado_indice")),
    )
    with op.batch_alter_table("resultado_indice", schema=None) as batch_op:
        batch_op.create_index(
            "idx_resultado_execucao_ano_indice",
            ["execucao_id", "ano", "nome_indice"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("resultado_indice", schema=None) as batch_op:
        batch_op.drop_index("idx_resultado_execucao_ano_indice")
    op.drop_table("resultado_indice")

    op.drop_table("fornecedor")
    op.drop_table("execucao")

    with op.batch_alter_table("municipio", schema=None) as batch_op:
        batch_op.drop_index("idx_municipio_uf_nome_normalizado")
    op.drop_table("municipio")

    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_index(
            "idx_job_pending_fila",
            sqlite_where=sa.text("status = 'pending'"),
            postgresql_where=sa.text("status = 'pending'"),
        )
    op.drop_table("job")
