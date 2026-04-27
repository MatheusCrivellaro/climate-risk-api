"""Slice 19: rename ``intensidade_mm`` para ``intensidade_mm_dia`` e zera tabela.

Revision ID: 5c2d8a17b9f4
Revises: 3e5b1c0d4f22
Create Date: 2026-04-27

Mudança da definição de intensidade (Slice 19, ADR-011): a coluna
``intensidade_mm`` (que armazenava a *soma* anual do déficit hídrico nos
dias secos quentes) passa a se chamar ``intensidade_mm_dia`` e
representará a *média por dia* (mm/dia). Não há recálculo dos dados
existentes — a tabela é zerada porque a única execução existente era
de teste.

SQLite suporta ``ALTER TABLE ... RENAME COLUMN`` desde a versão 3.25
(2018), então o rename é direto e portável para PostgreSQL.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "5c2d8a17b9f4"
down_revision: str | Sequence[str] | None = "3e5b1c0d4f22"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Apaga todos os registros existentes — a definição de intensidade
    # mudou e os valores armazenados (soma) não são compatíveis com a
    # nova semântica (média por dia). Não há recálculo: a única execução
    # atual era de teste e será refeita.
    op.execute("DELETE FROM resultado_estresse_hidrico")
    op.execute(
        "ALTER TABLE resultado_estresse_hidrico "
        "RENAME COLUMN intensidade_mm TO intensidade_mm_dia"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE resultado_estresse_hidrico "
        "RENAME COLUMN intensidade_mm_dia TO intensidade_mm"
    )
