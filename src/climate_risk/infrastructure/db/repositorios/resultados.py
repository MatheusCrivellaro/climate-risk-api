"""Implementação SQLAlchemy de :class:`RepositorioResultados`."""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
    GrupoAgregadoRaw,
)
from climate_risk.infrastructure.db.modelos import (
    ExecucaoORM,
    MunicipioORM,
    ResultadoIndiceORM,
)

_AGREGACOES_SQL = {"media", "min", "max", "count"}
_AGREGACOES_PYTHON = {"p50", "p95"}
_AGREGACOES_VALIDAS = _AGREGACOES_SQL | _AGREGACOES_PYTHON

_DIMENSOES_VALIDAS = {"ano", "cenario", "variavel", "nome_indice", "municipio"}


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

    async def municipios_com_resultados(self, municipios_ids: set[int]) -> set[int]:
        """Intersecta ``municipios_ids`` com os IDs presentes em ``resultado_indice``.

        Entrada vazia devolve ``set()`` sem tocar no banco. SQLite lida com
        ``IN (...)`` de centenas de itens sem problema; para milhares, o
        caller deve chunkizar por fora (não é o caso atual do Slice 9).
        """
        if not municipios_ids:
            return set()
        stmt = (
            select(ResultadoIndiceORM.municipio_id)
            .where(ResultadoIndiceORM.municipio_id.in_(municipios_ids))
            .where(ResultadoIndiceORM.municipio_id.is_not(None))
            .distinct()
        )
        resultado = await self._sessao.execute(stmt)
        return {int(row) for row in resultado.scalars().all() if row is not None}

    def _aplicar_filtros_dto(self, stmt: Any, filtros: FiltrosConsultaResultados) -> Any:
        """Variante do ``_aplicar_filtros`` que aceita o DTO do Slice 11.

        Diferenças-chave vs. ``_aplicar_filtros``:

        - ``nomes_indices`` é tupla → ``IN(...)`` quando não vazia;
        - ``ano`` exato é suportado separadamente de ``ano_min/ano_max``;
        - BBOX inline (lat/lon); cruza antimeridiano quando ``lon_min >
          lon_max``.

        Anexamos JOINs preguiçosamente para não duplicar linhas quando o
        filtro não exige a tabela correspondente.
        """
        if filtros.execucao_id is not None:
            stmt = stmt.where(ResultadoIndiceORM.execucao_id == filtros.execucao_id)
        if filtros.nomes_indices:
            stmt = stmt.where(ResultadoIndiceORM.nome_indice.in_(filtros.nomes_indices))
        if filtros.ano is not None:
            stmt = stmt.where(ResultadoIndiceORM.ano == filtros.ano)
        if filtros.ano_min is not None:
            stmt = stmt.where(ResultadoIndiceORM.ano >= filtros.ano_min)
        if filtros.ano_max is not None:
            stmt = stmt.where(ResultadoIndiceORM.ano <= filtros.ano_max)
        if filtros.municipio_id is not None:
            stmt = stmt.where(ResultadoIndiceORM.municipio_id == filtros.municipio_id)

        if filtros.cenario is not None or filtros.variavel is not None:
            stmt = stmt.join(ExecucaoORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
            if filtros.cenario is not None:
                stmt = stmt.where(ExecucaoORM.cenario == filtros.cenario)
            if filtros.variavel is not None:
                stmt = stmt.where(ExecucaoORM.variavel == filtros.variavel)

        if filtros.uf is not None:
            stmt = stmt.join(MunicipioORM, ResultadoIndiceORM.municipio_id == MunicipioORM.id)
            stmt = stmt.where(MunicipioORM.uf == filtros.uf)

        if (
            filtros.lat_min is not None
            and filtros.lat_max is not None
            and filtros.lon_min is not None
            and filtros.lon_max is not None
        ):
            lat_ok = and_(
                ResultadoIndiceORM.lat >= filtros.lat_min,
                ResultadoIndiceORM.lat <= filtros.lat_max,
            )
            if filtros.lon_min <= filtros.lon_max:
                lon_ok = and_(
                    ResultadoIndiceORM.lon >= filtros.lon_min,
                    ResultadoIndiceORM.lon <= filtros.lon_max,
                )
            else:
                # BBox cruza o antimeridiano: duas faixas disjuntas em longitude.
                lon_ok = or_(
                    ResultadoIndiceORM.lon >= filtros.lon_min,
                    ResultadoIndiceORM.lon <= filtros.lon_max,
                )
            stmt = stmt.where(and_(lat_ok, lon_ok))

        return stmt

    async def consultar(
        self,
        filtros: FiltrosConsultaResultados,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        stmt = select(ResultadoIndiceORM)
        stmt = self._aplicar_filtros_dto(stmt, filtros)
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

    async def contar_por_filtros(self, filtros: FiltrosConsultaResultados) -> int:
        stmt = select(func.count()).select_from(ResultadoIndiceORM)
        stmt = self._aplicar_filtros_dto(stmt, filtros)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())

    async def agregar(
        self, filtros_agregacao: FiltrosAgregacaoResultados
    ) -> list[GrupoAgregadoRaw]:
        """Executa ``GROUP BY`` + função de agregação.

        Para ``media``/``min``/``max``/``count`` a função é resolvida em
        SQL (``AVG``/``MIN``/``MAX``/``COUNT``). Para percentis
        (``p50``/``p95``) buscamos os valores por grupo em SQL e
        computamos o percentil em Python (SQLite não tem
        ``PERCENTILE_CONT``). ``count`` inclui linhas com ``valor IS
        NULL`` — as demais agregações as ignoram naturalmente.
        """
        agregacao = filtros_agregacao.agregacao
        if agregacao not in _AGREGACOES_VALIDAS:
            raise ValueError(f"Agregação inválida: {agregacao!r}")
        for dim in filtros_agregacao.agrupar_por:
            if dim not in _DIMENSOES_VALIDAS:
                raise ValueError(f"Dimensão de agrupamento inválida: {dim!r}")

        filtros = filtros_agregacao.filtros
        agrupar_por = filtros_agregacao.agrupar_por

        # Detecta se precisamos de JOIN com execucao por causa das dimensões.
        precisa_join_execucao = any(d in ("cenario", "variavel") for d in agrupar_por)

        colunas_grupo = self._mapear_colunas_grupo(agrupar_por)

        if agregacao in _AGREGACOES_PYTHON:
            return await self._agregar_percentil_python(
                filtros=filtros,
                agrupar_por=agrupar_por,
                agregacao=agregacao,
                precisa_join_execucao=precisa_join_execucao,
                colunas_grupo=colunas_grupo,
            )
        return await self._agregar_sql(
            filtros=filtros,
            agrupar_por=agrupar_por,
            agregacao=agregacao,
            precisa_join_execucao=precisa_join_execucao,
            colunas_grupo=colunas_grupo,
        )

    @staticmethod
    def _mapear_colunas_grupo(agrupar_por: tuple[str, ...]) -> list[Any]:
        mapa: dict[str, Any] = {
            "ano": ResultadoIndiceORM.ano,
            "nome_indice": ResultadoIndiceORM.nome_indice,
            "municipio": ResultadoIndiceORM.municipio_id,
            "cenario": ExecucaoORM.cenario,
            "variavel": ExecucaoORM.variavel,
        }
        return [mapa[dim] for dim in agrupar_por]

    async def _agregar_sql(
        self,
        filtros: FiltrosConsultaResultados,
        agrupar_por: tuple[str, ...],
        agregacao: str,
        precisa_join_execucao: bool,
        colunas_grupo: list[Any],
    ) -> list[GrupoAgregadoRaw]:
        valor_col = ResultadoIndiceORM.valor
        if agregacao == "media":
            agg_col = func.avg(valor_col)
        elif agregacao == "min":
            agg_col = func.min(valor_col)
        elif agregacao == "max":
            agg_col = func.max(valor_col)
        else:  # count
            agg_col = func.count()
        n_amostras_col = func.count(valor_col) if agregacao != "count" else func.count()

        colunas = [*colunas_grupo, agg_col.label("valor"), n_amostras_col.label("n_amostras")]
        stmt = select(*colunas).select_from(ResultadoIndiceORM)
        if precisa_join_execucao:
            stmt = stmt.join(ExecucaoORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
        stmt = self._aplicar_filtros_dto(stmt, filtros)
        if colunas_grupo:
            stmt = stmt.group_by(*colunas_grupo).order_by(*colunas_grupo)

        resultado = await self._sessao.execute(stmt)
        saida: list[GrupoAgregadoRaw] = []
        for linha in resultado.all():
            valores = list(linha)
            n_amostras = int(valores[-1])
            valor_bruto = valores[-2]
            chaves = valores[:-2]
            grupo = self._construir_dict_grupo(agrupar_por, chaves)
            saida.append(
                GrupoAgregadoRaw(
                    grupo=grupo,
                    valor=float(valor_bruto) if valor_bruto is not None else None,
                    n_amostras=n_amostras,
                )
            )
        return saida

    async def _agregar_percentil_python(
        self,
        filtros: FiltrosConsultaResultados,
        agrupar_por: tuple[str, ...],
        agregacao: str,
        precisa_join_execucao: bool,
        colunas_grupo: list[Any],
    ) -> list[GrupoAgregadoRaw]:
        """Busca valores brutos por grupo e computa o percentil em Python."""
        colunas = [*colunas_grupo, ResultadoIndiceORM.valor]
        stmt = select(*colunas).select_from(ResultadoIndiceORM)
        if precisa_join_execucao:
            stmt = stmt.join(ExecucaoORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
        stmt = self._aplicar_filtros_dto(stmt, filtros)
        if colunas_grupo:
            stmt = stmt.order_by(*colunas_grupo)

        resultado = await self._sessao.execute(stmt)
        grupos: dict[tuple[Any, ...], list[float]] = {}
        for linha in resultado.all():
            valores = list(linha)
            valor_bruto = valores[-1]
            chaves = tuple(valores[:-1])
            grupos.setdefault(chaves, [])
            if valor_bruto is not None:
                grupos[chaves].append(float(valor_bruto))

        quantil = 0.5 if agregacao == "p50" else 0.95
        saida: list[GrupoAgregadoRaw] = []
        for chaves, valores in grupos.items():
            grupo = self._construir_dict_grupo(agrupar_por, list(chaves))
            valor_perc = _percentil(valores, quantil) if valores else None
            saida.append(
                GrupoAgregadoRaw(
                    grupo=grupo,
                    valor=valor_perc,
                    n_amostras=len(valores),
                )
            )
        return saida

    @staticmethod
    def _construir_dict_grupo(
        agrupar_por: tuple[str, ...], chaves: list[Any]
    ) -> dict[str, str | int]:
        grupo: dict[str, str | int] = {}
        for dim, chave in zip(agrupar_por, chaves, strict=True):
            if chave is None:
                continue
            if dim in ("ano", "municipio"):
                grupo[dim] = int(chave)
            else:
                grupo[dim] = str(chave)
        return grupo

    async def distinct_cenarios(self) -> list[str]:
        stmt = (
            select(ExecucaoORM.cenario)
            .join(ResultadoIndiceORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
            .distinct()
            .order_by(ExecucaoORM.cenario)
        )
        resultado = await self._sessao.execute(stmt)
        return [str(v) for v in resultado.scalars().all() if v is not None]

    async def distinct_anos(self) -> list[int]:
        stmt = select(ResultadoIndiceORM.ano).distinct().order_by(ResultadoIndiceORM.ano)
        resultado = await self._sessao.execute(stmt)
        return [int(v) for v in resultado.scalars().all() if v is not None]

    async def distinct_variaveis(self) -> list[str]:
        stmt = (
            select(ExecucaoORM.variavel)
            .join(ResultadoIndiceORM, ResultadoIndiceORM.execucao_id == ExecucaoORM.id)
            .distinct()
            .order_by(ExecucaoORM.variavel)
        )
        resultado = await self._sessao.execute(stmt)
        return [str(v) for v in resultado.scalars().all() if v is not None]

    async def distinct_nomes_indices(self) -> list[str]:
        stmt = (
            select(ResultadoIndiceORM.nome_indice)
            .distinct()
            .order_by(ResultadoIndiceORM.nome_indice)
        )
        resultado = await self._sessao.execute(stmt)
        return [str(v) for v in resultado.scalars().all() if v is not None]

    async def contar_execucoes_com_resultados(self) -> int:
        stmt = select(func.count(func.distinct(ResultadoIndiceORM.execucao_id)))
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())

    async def contar_resultados(self) -> int:
        stmt = select(func.count()).select_from(ResultadoIndiceORM)
        resultado = await self._sessao.execute(stmt)
        return int(resultado.scalar_one())


def _percentil(valores: list[float], quantil: float) -> float:
    """Percentil por interpolação linear (``statistics.quantiles``).

    Usa o método ``"inclusive"`` para ficar consistente com ``numpy.percentile``
    com ``linear`` em listas pequenas. Para listas de 1 elemento,
    ``statistics.quantiles`` erra; tratamos o caso explicitamente.
    """
    if len(valores) == 1:
        return valores[0]
    ordenados = sorted(valores)
    # 99 pontos de corte dão resolução de 1% — suficiente para p50 e p95.
    cortes = statistics.quantiles(ordenados, n=100, method="inclusive")
    indice = round(quantil * 100) - 1
    return float(cortes[indice])
