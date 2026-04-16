"""Implementação SQLAlchemy de :class:`RepositorioJobs` (CRUD apenas)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.job import Job
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import JobORM


class SQLAlchemyRepositorioJobs:
    """CRUD de jobs. Operações de fila (acquire, heartbeat) ficam para o Slice 5."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: JobORM) -> Job:
        payload: dict[str, Any] = json.loads(orm.payload) if orm.payload else {}
        criado_em = iso_para_datetime(orm.criado_em)
        assert criado_em is not None
        return Job(
            id=orm.id,
            tipo=orm.tipo,
            payload=payload,
            status=orm.status,
            tentativas=orm.tentativas,
            max_tentativas=orm.max_tentativas,
            criado_em=criado_em,
            iniciado_em=iso_para_datetime(orm.iniciado_em),
            concluido_em=iso_para_datetime(orm.concluido_em),
            heartbeat=iso_para_datetime(orm.heartbeat),
            erro=orm.erro,
            proxima_tentativa_em=iso_para_datetime(orm.proxima_tentativa_em),
        )

    @staticmethod
    def _to_valores(entidade: Job) -> dict[str, Any]:
        criado_em_iso = datetime_para_iso(entidade.criado_em)
        assert criado_em_iso is not None
        return {
            "id": entidade.id,
            "tipo": entidade.tipo,
            "payload": json.dumps(entidade.payload),
            "status": entidade.status,
            "tentativas": entidade.tentativas,
            "max_tentativas": entidade.max_tentativas,
            "criado_em": criado_em_iso,
            "iniciado_em": datetime_para_iso(entidade.iniciado_em),
            "concluido_em": datetime_para_iso(entidade.concluido_em),
            "heartbeat": datetime_para_iso(entidade.heartbeat),
            "erro": entidade.erro,
            "proxima_tentativa_em": datetime_para_iso(entidade.proxima_tentativa_em),
        }

    async def buscar_por_id(self, job_id: str) -> Job | None:
        orm = await self._sessao.get(JobORM, job_id)
        return self._to_domain(orm) if orm else None

    async def salvar(self, job: Job) -> None:
        """Upsert por ``id``."""
        valores = self._to_valores(job)
        stmt = sqlite_insert(JobORM).values(**valores)
        stmt = stmt.on_conflict_do_update(
            index_elements=[JobORM.id],
            set_={k: v for k, v in valores.items() if k != "id"},
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()

    async def listar(
        self,
        status: str | None = None,
        tipo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        stmt = select(JobORM)
        if status is not None:
            stmt = stmt.where(JobORM.status == status)
        if tipo is not None:
            stmt = stmt.where(JobORM.tipo == tipo)
        stmt = stmt.order_by(JobORM.criado_em.desc()).limit(limit).offset(offset)
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(
        self,
        status: str | None = None,
        tipo: str | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(JobORM)
        if status is not None:
            stmt = stmt.where(JobORM.status == status)
        if tipo is not None:
            stmt = stmt.where(JobORM.tipo == tipo)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())
