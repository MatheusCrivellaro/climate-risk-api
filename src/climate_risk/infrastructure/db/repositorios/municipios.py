"""Implementação SQLAlchemy de :class:`RepositorioMunicipios`."""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.infrastructure.db.modelos import MunicipioORM


class SQLAlchemyRepositorioMunicipios:
    """Repositório de municípios usando SQLite via SQLAlchemy async."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: MunicipioORM) -> Municipio:
        return Municipio(
            id=orm.id,
            nome=orm.nome,
            nome_normalizado=orm.nome_normalizado,
            uf=orm.uf,
            lat_centroide=orm.lat_centroide,
            lon_centroide=orm.lon_centroide,
        )

    async def buscar_por_id(self, municipio_id: int) -> Municipio | None:
        orm = await self._sessao.get(MunicipioORM, municipio_id)
        return self._to_domain(orm) if orm else None

    async def buscar_por_nome_uf(self, nome_normalizado: str, uf: str) -> Municipio | None:
        stmt = select(MunicipioORM).where(
            MunicipioORM.nome_normalizado == nome_normalizado,
            MunicipioORM.uf == uf,
        )
        resultado = await self._sessao.execute(stmt)
        orm = resultado.scalar_one_or_none()
        return self._to_domain(orm) if orm else None

    async def salvar(self, municipio: Municipio) -> None:
        """Upsert por ``id`` (INSERT ... ON CONFLICT DO UPDATE)."""
        valores = {
            "id": municipio.id,
            "nome": municipio.nome,
            "nome_normalizado": municipio.nome_normalizado,
            "uf": municipio.uf,
            "lat_centroide": municipio.lat_centroide,
            "lon_centroide": municipio.lon_centroide,
        }
        stmt = sqlite_insert(MunicipioORM).values(**valores)
        stmt = stmt.on_conflict_do_update(
            index_elements=[MunicipioORM.id],
            set_={k: v for k, v in valores.items() if k != "id"},
        )
        await self._sessao.execute(stmt)
        await self._sessao.commit()

    async def listar(
        self,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Municipio]:
        stmt = select(MunicipioORM)
        if uf is not None:
            stmt = stmt.where(MunicipioORM.uf == uf)
        stmt = stmt.order_by(MunicipioORM.nome_normalizado).limit(limit).offset(offset)
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(self, uf: str | None = None) -> int:
        stmt = select(func.count()).select_from(MunicipioORM)
        if uf is not None:
            stmt = stmt.where(MunicipioORM.uf == uf)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())

    async def remover(self, municipio_id: int) -> bool:
        stmt = delete(MunicipioORM).where(MunicipioORM.id == municipio_id)
        resultado = await self._sessao.execute(stmt)
        await self._sessao.commit()
        return bool(resultado.rowcount)  # type: ignore[attr-defined]
