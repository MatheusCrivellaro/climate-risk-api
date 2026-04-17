"""Teste de ``alembic upgrade head`` e ``downgrade base`` contra banco real.

Usa ``subprocess`` para rodar o CLI do Alembic sobre um banco temporário em
arquivo. Valida que:

1. ``upgrade head`` cria as 5 tabelas e os índices;
2. ``downgrade base`` remove tudo limpamente;
3. ``upgrade head`` de novo é idempotente (reexecuta sem erro).

Não executamos em :memory: porque precisamos persistir entre invocações do
CLI.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

RAIZ_PROJETO = Path(__file__).resolve().parents[3]

TABELAS_ESPERADAS = {
    "job",
    "municipio",
    "execucao",
    "fornecedor",
    "resultado_indice",
}

INDICES_ESPERADOS = {
    "idx_job_pending_fila",
    "idx_municipio_uf_nome_normalizado",
    "idx_resultado_execucao_ano_indice",
}


def _tabelas(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}


def _indices(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}


def _alembic(db_file: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["CLIMATE_RISK_DATABASE_URL"] = f"sqlite+aiosqlite:///{db_file.as_posix()}"
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=RAIZ_PROJETO,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )


@pytest.mark.skipif(
    shutil.which(sys.executable) is None,
    reason="Interpretador Python não disponível",
)
def test_upgrade_downgrade_upgrade_limpa_e_recria() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = Path(tmpdir) / "test_migration.db"

        _alembic(db_file, "upgrade", "head")
        tabelas = _tabelas(db_file)
        indices = _indices(db_file)
        assert TABELAS_ESPERADAS.issubset(tabelas), (
            f"Tabelas faltando: {TABELAS_ESPERADAS - tabelas}"
        )
        assert INDICES_ESPERADOS.issubset(indices), (
            f"Índices faltando: {INDICES_ESPERADOS - indices}"
        )

        _alembic(db_file, "downgrade", "base")
        tabelas_depois = _tabelas(db_file)
        assert not (TABELAS_ESPERADAS & tabelas_depois), (
            f"Tabelas persistiram após downgrade: {TABELAS_ESPERADAS & tabelas_depois}"
        )

        _alembic(db_file, "upgrade", "head")
        tabelas_final = _tabelas(db_file)
        assert TABELAS_ESPERADAS.issubset(tabelas_final)
