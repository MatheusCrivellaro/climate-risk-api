"""Implementação SQLAlchemy de :class:`RepositorioExecucoes`."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.execucao import Execucao
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import ExecucaoORM


class SQLAlchemyRepositorioExecucoes:
    """CRUD de execuções."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: ExecucaoORM) -> Execucao:
        parametros: dict[str, Any] = json.loads(orm.parametros) if orm.parametros else {}
        criado_em = iso_para_datetime(orm.criado_em)
        assert criado_em is not None
        return Execucao(
            id=orm.id,
            cenario=orm.cenario,
            variavel=orm.variavel,
            arquivo_origem=orm.arquivo_origem,
            tipo=orm.tipo,
            parametros=parametros,
            status=orm.status,
            criado_em=criado_em,
            concluido_em=iso_para_datetime(orm.concluido_em),
            job_id=orm.job_id,
        )

    @staticmethod
    def _to_valores(entidade: Execucao) -> dict[str, Any]:
        criado_em_iso = datetime_para_iso(entidade.criado_em)
        assert criado_em_iso is not None
        return {
            "id": entidade.id,
            "cenario": entidade.cenario,
            "variavel": entidade.variavel,
            "arquivo_origem": entidade.arquivo_origem,
            "tipo": entidade.tipo,
            "parametros": json.dumps(entidade.parametros),
            "status": entidade.status,
            "criado_em": criado_em_iso,
            "concluido_em": datetime_para_iso(entidade.concluido_em),
            "job_id": entidade.job_id,
        }

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        orm = await self._sessao.get(ExecucaoORM, execucao_id)
        return self._to_domain(orm) if orm else None

    async def salvar(self, execucao: Execucao) -> None:
        """Upsert por ``id``."""
        valores = self._to_valores(execucao)
        stmt = sqlite_insert(ExecucaoORM).values(**valores)
        stmt = stmt.on_conflict_do_update(
            index_elements=[ExecucaoORM.id],
            set_={k: v for k, v in valores.items() if k != "id"},
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()

    async def listar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Execucao]:
        stmt = select(ExecucaoORM)
        if cenario is not None:
            stmt = stmt.where(ExecucaoORM.cenario == cenario)
        if variavel is not None:
            stmt = stmt.where(ExecucaoORM.variavel == variavel)
        if status is not None:
            stmt = stmt.where(ExecucaoORM.status == status)
        stmt = stmt.order_by(ExecucaoORM.criado_em.desc()).limit(limit).offset(offset)
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(ExecucaoORM)
        if cenario is not None:
            stmt = stmt.where(ExecucaoORM.cenario == cenario)
        if variavel is not None:
            stmt = stmt.where(ExecucaoORM.variavel == variavel)
        if status is not None:
            stmt = stmt.where(ExecucaoORM.status == status)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())
