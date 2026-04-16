"""Implementação SQLAlchemy de :class:`RepositorioFornecedores`."""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroConflito
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import FornecedorORM


class SQLAlchemyRepositorioFornecedores:
    """CRUD de fornecedores."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: FornecedorORM) -> Fornecedor:
        criado_em = iso_para_datetime(orm.criado_em)
        assert criado_em is not None  # coluna NOT NULL
        return Fornecedor(
            id=orm.id,
            identificador_externo=orm.identificador_externo,
            nome=orm.nome,
            lat=orm.lat,
            lon=orm.lon,
            municipio_id=orm.municipio_id,
            criado_em=criado_em,
        )

    @staticmethod
    def _to_model(entidade: Fornecedor) -> FornecedorORM:
        criado_em_iso = datetime_para_iso(entidade.criado_em)
        assert criado_em_iso is not None
        return FornecedorORM(
            id=entidade.id,
            identificador_externo=entidade.identificador_externo,
            nome=entidade.nome,
            lat=entidade.lat,
            lon=entidade.lon,
            municipio_id=entidade.municipio_id,
            criado_em=criado_em_iso,
        )

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor | None:
        orm = await self._sessao.get(FornecedorORM, fornecedor_id)
        return self._to_domain(orm) if orm else None

    async def salvar(self, fornecedor: Fornecedor) -> None:
        """Insere — erro se já existe (semântica de unicidade de ``id``)."""
        try:
            self._sessao.add(self._to_model(fornecedor))
            await self._sessao.commit()
        except IntegrityError as erro:
            await self._sessao.rollback()
            raise ErroConflito(
                f"Fornecedor '{fornecedor.id}' já existe ou viola integridade."
            ) from erro

    async def listar(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Fornecedor]:
        stmt = (
            select(FornecedorORM)
            .order_by(FornecedorORM.criado_em.desc(), FornecedorORM.id)
            .limit(limit)
            .offset(offset)
        )
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(self) -> int:
        stmt = select(func.count()).select_from(FornecedorORM)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())

    async def remover(self, fornecedor_id: str) -> bool:
        stmt = delete(FornecedorORM).where(FornecedorORM.id == fornecedor_id)
        resultado = await self._sessao.execute(stmt)
        await self._sessao.commit()
        return bool(resultado.rowcount)  # type: ignore[attr-defined]
