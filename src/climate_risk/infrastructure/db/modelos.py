"""Modelos ORM (SQLAlchemy 2.x).

**Disciplina de portabilidade (ADR-003):**

- Timestamps são armazenados em ``String(32)`` no formato ISO 8601 UTC
  (ex.: ``"2026-04-16T10:30:00+00:00"``). Nunca usamos ``REAL``/Julian.
- Campos JSON (``parametros``, ``payload``) são armazenados em ``Text`` com
  serialização explícita (``json.dumps`` / ``json.loads``). A migração
  futura para PostgreSQL ``JSONB`` troca apenas o tipo da coluna — o código
  de leitura/escrita continua o mesmo.
- IDs são gerados na aplicação (``core/ids.py``). Não usamos ``AUTOINCREMENT``
  exceto para ``municipio.id``, que é o código IBGE real (fornecido externamente).
- ``CHECK constraints`` restringem enums de status.
- Índices seguem ``docs/desenho-api.md`` seção 5.

Os modelos nunca vazam para fora de ``infrastructure/db/``. Repositórios
fazem a tradução ORM ↔ dataclass de domínio.
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from climate_risk.domain.entidades.execucao import StatusExecucao
from climate_risk.domain.entidades.job import StatusJob
from climate_risk.infrastructure.db.base import Base

_STATUS_EXECUCAO_LISTA = ",".join(f"'{s}'" for s in StatusExecucao.TODOS)
_STATUS_JOB_LISTA = ",".join(f"'{s}'" for s in StatusJob.TODOS)


class MunicipioORM(Base):
    __tablename__ = "municipio"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    nome: Mapped[str] = mapped_column(String(120), nullable=False)
    nome_normalizado: Mapped[str] = mapped_column(String(120), nullable=False)
    uf: Mapped[str] = mapped_column(String(2), nullable=False)
    lat_centroide: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon_centroide: Mapped[float | None] = mapped_column(Float, nullable=True)
    atualizado_em: Mapped[str] = mapped_column(String(32), nullable=False)

    __table_args__ = (Index("idx_municipio_uf_nome_normalizado", "uf", "nome_normalizado"),)


class FornecedorORM(Base):
    __tablename__ = "fornecedor"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    identificador_externo: Mapped[str | None] = mapped_column(String(120), nullable=True)
    nome: Mapped[str] = mapped_column(String(200), nullable=False)
    cidade: Mapped[str] = mapped_column(String(120), nullable=False)
    uf: Mapped[str] = mapped_column(String(2), nullable=False)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon: Mapped[float | None] = mapped_column(Float, nullable=True)
    municipio_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("municipio.id", ondelete="SET NULL"),
        nullable=True,
    )
    criado_em: Mapped[str] = mapped_column(String(32), nullable=False)
    atualizado_em: Mapped[str] = mapped_column(String(32), nullable=False)

    __table_args__ = (
        Index("idx_fornecedor_uf_cidade", "uf", "cidade"),
        Index("idx_fornecedor_nome_cidade_uf", "nome", "cidade", "uf"),
    )


class ExecucaoORM(Base):
    __tablename__ = "execucao"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    cenario: Mapped[str] = mapped_column(String(40), nullable=False)
    variavel: Mapped[str] = mapped_column(String(20), nullable=False)
    arquivo_origem: Mapped[str] = mapped_column(Text, nullable=False)
    tipo: Mapped[str] = mapped_column(String(20), nullable=False)
    parametros: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=StatusExecucao.PENDING)
    criado_em: Mapped[str] = mapped_column(String(32), nullable=False)
    concluido_em: Mapped[str | None] = mapped_column(String(32), nullable=True)
    job_id: Mapped[str | None] = mapped_column(
        String(40),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            f"status IN ({_STATUS_EXECUCAO_LISTA})",
            name="status_valido",
        ),
    )


class ResultadoIndiceORM(Base):
    __tablename__ = "resultado_indice"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    execucao_id: Mapped[str] = mapped_column(
        String(40),
        ForeignKey("execucao.id", ondelete="CASCADE"),
        nullable=False,
    )
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    lon: Mapped[float] = mapped_column(Float, nullable=False)
    lat_input: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon_input: Mapped[float | None] = mapped_column(Float, nullable=True)
    ano: Mapped[int] = mapped_column(Integer, nullable=False)
    nome_indice: Mapped[str] = mapped_column(String(40), nullable=False)
    valor: Mapped[float | None] = mapped_column(Float, nullable=True)
    unidade: Mapped[str] = mapped_column(String(20), nullable=False)
    municipio_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("municipio.id", ondelete="SET NULL"),
        nullable=True,
    )

    __table_args__ = (
        Index(
            "idx_resultado_execucao_ano_indice",
            "execucao_id",
            "ano",
            "nome_indice",
        ),
    )


class ResultadoEstresseHidricoORM(Base):
    """Tabela em formato **wide** (frequência + intensidade por linha).

    Desacoplada de ``resultado_indice`` de propósito — o pipeline de
    estresse hídrico agrega frequência e intensidade num único registro por
    ``(execucao, municipio, ano)``. Ver :class:`ResultadoEstresseHidrico`.
    """

    __tablename__ = "resultado_estresse_hidrico"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    execucao_id: Mapped[str] = mapped_column(
        String(40),
        ForeignKey("execucao.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    municipio_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    ano: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    cenario: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    frequencia_dias_secos_quentes: Mapped[int] = mapped_column(Integer, nullable=False)
    intensidade_mm_dia: Mapped[float] = mapped_column(Float, nullable=False)
    criado_em: Mapped[str] = mapped_column(String(32), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "execucao_id",
            "municipio_id",
            "ano",
            name="uq_resultado_estresse_hidrico_execucao_mun_ano",
        ),
    )


class JobORM(Base):
    __tablename__ = "job"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    tipo: Mapped[str] = mapped_column(String(60), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=StatusJob.PENDING)
    tentativas: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_tentativas: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    criado_em: Mapped[str] = mapped_column(String(32), nullable=False)
    iniciado_em: Mapped[str | None] = mapped_column(String(32), nullable=True)
    concluido_em: Mapped[str | None] = mapped_column(String(32), nullable=True)
    heartbeat: Mapped[str | None] = mapped_column(String(32), nullable=True)
    erro: Mapped[str | None] = mapped_column(Text, nullable=True)
    proxima_tentativa_em: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Índice parcial para a fila: apenas pending. SQLite e PostgreSQL suportam.
    # ``sqlite_where`` e ``postgresql_where`` declaram o mesmo predicado em
    # ambos os dialetos — migração futura troca apenas o backend.
    __table_args__ = (
        CheckConstraint(
            f"status IN ({_STATUS_JOB_LISTA})",
            name="status_valido",
        ),
        Index(
            "idx_job_pending_fila",
            "status",
            "proxima_tentativa_em",
            sqlite_where=text("status = 'pending'"),
            postgresql_where=text("status = 'pending'"),
        ),
    )
