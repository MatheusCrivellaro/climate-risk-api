"""Implementação SQLAlchemy de :class:`RepositorioResultadoEstresseHidrico`.

Persiste e consulta a tabela ``resultado_estresse_hidrico`` (formato wide,
Slice 15). Mantido separado de :class:`SQLAlchemyRepositorioResultados` —
tabelas distintas, filtros distintos.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)
from climate_risk.infrastructure.db.conversores_tempo import (
    datetime_para_iso,
    iso_para_datetime,
)
from climate_risk.infrastructure.db.modelos import (
    MunicipioORM,
    ResultadoEstresseHidricoORM,
)


class SQLAlchemyRepositorioResultadoEstresseHidrico:
    """Persistência em lote e consulta de resultados de estresse hídrico."""

    def __init__(self, sessao: AsyncSession) -> None:
        self._sessao = sessao

    @staticmethod
    def _to_domain(orm: ResultadoEstresseHidricoORM) -> ResultadoEstresseHidrico:
        criado_em = iso_para_datetime(orm.criado_em)
        assert criado_em is not None
        return ResultadoEstresseHidrico(
            id=orm.id,
            execucao_id=orm.execucao_id,
            municipio_id=orm.municipio_id,
            ano=orm.ano,
            cenario=orm.cenario,
            frequencia_dias_secos_quentes=orm.frequencia_dias_secos_quentes,
            intensidade_mm=orm.intensidade_mm,
            criado_em=criado_em,
        )

    @staticmethod
    def _to_model(entidade: ResultadoEstresseHidrico) -> ResultadoEstresseHidricoORM:
        criado_em_iso = datetime_para_iso(entidade.criado_em)
        assert criado_em_iso is not None
        return ResultadoEstresseHidricoORM(
            id=entidade.id,
            execucao_id=entidade.execucao_id,
            municipio_id=entidade.municipio_id,
            ano=entidade.ano,
            cenario=entidade.cenario,
            frequencia_dias_secos_quentes=entidade.frequencia_dias_secos_quentes,
            intensidade_mm=entidade.intensidade_mm,
            criado_em=criado_em_iso,
        )

    async def salvar_lote(
        self,
        resultados: Iterable[ResultadoEstresseHidrico],
    ) -> None:
        lista = list(resultados)
        if not lista:
            return
        self._sessao.add_all([self._to_model(r) for r in lista])
        await self._sessao.commit()

    def _aplicar_filtros(
        self,
        stmt: Any,
        *,
        execucao_id: str | None,
        cenario: str | None,
        ano: int | None,
        ano_min: int | None,
        ano_max: int | None,
        municipio_id: int | None,
        uf: str | None,
    ) -> Any:
        if execucao_id is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.execucao_id == execucao_id)
        if cenario is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.cenario == cenario)
        if ano is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano == ano)
        if ano_min is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano >= ano_min)
        if ano_max is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano <= ano_max)
        if municipio_id is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.municipio_id == municipio_id)
        if uf is not None:
            stmt = stmt.join(
                MunicipioORM,
                ResultadoEstresseHidricoORM.municipio_id == MunicipioORM.id,
            ).where(MunicipioORM.uf == uf)
        return stmt

    async def listar(
        self,
        *,
        execucao_id: str | None = None,
        cenario: str | None = None,
        ano: int | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        municipio_id: int | None = None,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoEstresseHidrico]:
        stmt = select(ResultadoEstresseHidricoORM)
        stmt = self._aplicar_filtros(
            stmt,
            execucao_id=execucao_id,
            cenario=cenario,
            ano=ano,
            ano_min=ano_min,
            ano_max=ano_max,
            municipio_id=municipio_id,
            uf=uf,
        )
        stmt = (
            stmt.order_by(
                ResultadoEstresseHidricoORM.execucao_id,
                ResultadoEstresseHidricoORM.ano,
                ResultadoEstresseHidricoORM.municipio_id,
                ResultadoEstresseHidricoORM.id,
            )
            .limit(limit)
            .offset(offset)
        )
        resultado = await self._sessao.execute(stmt)
        return [self._to_domain(orm) for orm in resultado.scalars().all()]

    async def contar(
        self,
        *,
        execucao_id: str | None = None,
        cenario: str | None = None,
        ano: int | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        municipio_id: int | None = None,
        uf: str | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(ResultadoEstresseHidricoORM)
        stmt = self._aplicar_filtros(
            stmt,
            execucao_id=execucao_id,
            cenario=cenario,
            ano=ano,
            ano_min=ano_min,
            ano_max=ano_max,
            municipio_id=municipio_id,
            uf=uf,
        )
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())

    async def listar_com_municipio(
        self,
        *,
        execucao_id: str | None = None,
        cenario: str | None = None,
        ano: int | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        municipio_id: int | None = None,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[ResultadoEstresseHidrico, str | None, str | None]]:
        """Variante que enriquece com ``(nome_municipio, uf)`` via LEFT JOIN.

        Útil para a API HTTP, que expõe esses campos opcionalmente na
        resposta. Não faz parte da porta de domínio (retorna tuplas — o
        caller HTTP mapeia para o schema Pydantic).
        """
        stmt = select(
            ResultadoEstresseHidricoORM,
            MunicipioORM.nome,
            MunicipioORM.uf,
        ).outerjoin(
            MunicipioORM,
            ResultadoEstresseHidricoORM.municipio_id == MunicipioORM.id,
        )
        if execucao_id is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.execucao_id == execucao_id)
        if cenario is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.cenario == cenario)
        if ano is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano == ano)
        if ano_min is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano >= ano_min)
        if ano_max is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.ano <= ano_max)
        if municipio_id is not None:
            stmt = stmt.where(ResultadoEstresseHidricoORM.municipio_id == municipio_id)
        if uf is not None:
            stmt = stmt.where(MunicipioORM.uf == uf)

        stmt = (
            stmt.order_by(
                ResultadoEstresseHidricoORM.execucao_id,
                ResultadoEstresseHidricoORM.ano,
                ResultadoEstresseHidricoORM.municipio_id,
                ResultadoEstresseHidricoORM.id,
            )
            .limit(limit)
            .offset(offset)
        )
        resultado = await self._sessao.execute(stmt)
        saida: list[tuple[ResultadoEstresseHidrico, str | None, str | None]] = []
        for orm, nome_mun, uf_mun in resultado.all():
            saida.append((self._to_domain(orm), nome_mun, uf_mun))
        return saida
