"""Implementação SQLAlchemy de :class:`RepositorioResultados`."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.infrastructure.db.modelos import (
    ExecucaoORM,
    MunicipioORM,
    ResultadoIndiceORM,
)


class SQLAlchemyRepositorioResultados:
    """Persistência em lote e consulta rica."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: ResultadoIndiceORM) -> ResultadoIndice:
        return ResultadoIndice(
            id=orm.id,
            execucao_id=orm.execucao_id,
            lat=orm.lat,
            lon=orm.lon,
            lat_input=orm.lat_input,
            lon_input=orm.lon_input,
            ano=orm.ano,
            nome_indice=orm.nome_indice,
            valor=orm.valor,
            unidade=orm.unidade,
            municipio_id=orm.municipio_id,
        )

    @staticmethod
    def _to_model(entidade: ResultadoIndice) -> ResultadoIndiceORM:
        return ResultadoIndiceORM(
            id=entidade.id,
            execucao_id=entidade.execucao_id,
            lat=entidade.lat,
            lon=entidade.lon,
            lat_input=entidade.lat_input,
            lon_input=entidade.lon_input,
            ano=entidade.ano,
            nome_indice=entidade.nome_indice,
            valor=entidade.valor,
            unidade=entidade.unidade,
            municipio_id=entidade.municipio_id,
        )

    async def salvar_lote(self, resultados: Sequence[ResultadoIndice]) -> None:
        if not resultados:
            return
        self._sessao.add_all([self._to_model(r) for r in resultados])
        await self._sessao.commit()

    def _aplicar_filtros(
        self,
        stmt: Any,
        execucao_id: str | None,
        cenario: str | None,
        variavel: str | None,
        ano_min: int | None,
        ano_max: int | None,
        nome_indice: str | None,
        bbox: BoundingBox | None,
        uf: str | None,
        municipio_id: int | None,
    ) -> Any:
        """Adiciona cláusulas WHERE/JOIN conforme filtros fornecidos.

        - Filtros de cenário/variável forçam JOIN com ``execucao`` (tabela referenciada).
        - Filtro de ``uf`` força JOIN com ``municipio``.
        - Filtro de bbox é aplicado em ``lat`` e ``lon``; se a bbox cruza o
          antimeridiano (``lon_min > lon_max``), convertemos para um OR
          (duas faixas disjuntas). Comentário explícito abaixo.
        """
        if execucao_id is not None:
            stmt = stmt.where(ResultadoIndiceORM.execucao_id == execucao_id)
        if nome_indice is not None:
            stmt = stmt.where(ResultadoIndiceORM.nome_indice == nome_indice)
        if ano_min is not None:
            stmt = stmt.where(ResultadoIndiceORM.ano >= ano_min)
        if ano_max is not None:
            stmt = stmt.where(ResultadoIndiceORM.ano <= ano_max)
        if municipio_id is not None:
            stmt = stmt.where(ResultadoIndiceORM.municipio_id == municipio_id)

        if cenario is not None or variavel is not None:
            stmt = stmt.join(ExecucaoORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
            if cenario is not None:
                stmt = stmt.where(ExecucaoORM.cenario == cenario)
            if variavel is not None:
                stmt = stmt.where(ExecucaoORM.variavel == variavel)

        if uf is not None:
            stmt = stmt.join(MunicipioORM, ResultadoIndiceORM.municipio_id == MunicipioORM.id)
            stmt = stmt.where(MunicipioORM.uf == uf)

        if bbox is not None:
            lat_ok = and_(
                ResultadoIndiceORM.lat >= bbox.lat_min,
                ResultadoIndiceORM.lat <= bbox.lat_max,
            )
            if bbox.lon_min <= bbox.lon_max:
                lon_ok = and_(
                    ResultadoIndiceORM.lon >= bbox.lon_min,
                    ResultadoIndiceORM.lon <= bbox.lon_max,
                )
            else:
                # BBox cruza antimeridiano: duas faixas disjuntas em longitude
                # (ex.: lon_min=170, lon_max=-170 -> [170,180] U [-180,-170]).
                lon_ok = or_(
                    ResultadoIndiceORM.lon >= bbox.lon_min,
                    ResultadoIndiceORM.lon <= bbox.lon_max,
                )
            stmt = stmt.where(and_(lat_ok, lon_ok))

        return stmt

    async def listar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        stmt = select(ResultadoIndiceORM)
        stmt = self._aplicar_filtros(
            stmt,
            execucao_id,
            cenario,
            variavel,
            ano_min,
            ano_max,
            nome_indice,
            bbox,
            uf,
            municipio_id,
        )
        stmt = (
            stmt.order_by(
                ResultadoIndiceORM.execucao_id,
                ResultadoIndiceORM.ano,
                ResultadoIndiceORM.nome_indice,
                ResultadoIndiceORM.id,
            )
            .limit(limit)
            .offset(offset)
        )
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(ResultadoIndiceORM)
        stmt = self._aplicar_filtros(
            stmt,
            execucao_id,
            cenario,
            variavel,
            ano_min,
            ano_max,
            nome_indice,
            bbox,
            uf,
            municipio_id,
        )
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())
