"""Rotas REST para consulta e agregação de resultados (``/resultados``).

Expõe os três endpoints do Slice 11 (Marco M4):

- ``GET /resultados``           → listagem paginada com filtros ricos.
- ``GET /resultados/agregados`` → agregações (media/min/max/count/p50/p95).
- ``GET /resultados/stats``     → dimensões disponíveis + counters.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from climate_risk.application.resultados import (
    AgregarResultados,
    ConsultarResultados,
    ConsultarStats,
    FiltrosAgregacao,
    FiltrosResultados,
    GrupoAgregado,
    PaginaResultados,
    ResultadoAgregacao,
)
from climate_risk.application.resultados.stats import EstatisticasResultados
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_agregar_resultados,
    obter_caso_uso_consultar_resultados,
    obter_caso_uso_consultar_stats,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.resultados import (
    AgregacaoResponse,
    EstatisticasResponse,
    GrupoAgregadoResponse,
    PaginaResultadosResponse,
    ResultadoResponse,
)

router = APIRouter(prefix="/resultados", tags=["resultados"])

ConsultarDep = Annotated[ConsultarResultados, Depends(obter_caso_uso_consultar_resultados)]
AgregarDep = Annotated[AgregarResultados, Depends(obter_caso_uso_agregar_resultados)]
StatsDep = Annotated[ConsultarStats, Depends(obter_caso_uso_consultar_stats)]


def _parse_csv(valor: str | None) -> tuple[str, ...]:
    """Quebra uma querystring CSV em tupla; vazia se ``None`` ou string vazia."""
    if not valor:
        return ()
    return tuple(item.strip() for item in valor.split(",") if item.strip())


@router.get(
    "",
    response_model=PaginaResultadosResponse,
    status_code=status.HTTP_200_OK,
    summary="Lista resultados com filtros ricos.",
    responses={
        422: {"model": ProblemDetails, "description": "Combinação inválida de filtros."},
    },
)
async def listar_resultados(
    caso_uso: ConsultarDep,
    execucao_id: Annotated[str | None, Query(description="Filtra por execução.")] = None,
    cenario: Annotated[str | None, Query(description="Cenário da execução.")] = None,
    variavel: Annotated[str | None, Query(description="Variável climática.")] = None,
    ano: Annotated[int | None, Query(description="Ano exato.")] = None,
    ano_min: Annotated[int | None, Query(description="Ano mínimo inclusivo.")] = None,
    ano_max: Annotated[int | None, Query(description="Ano máximo inclusivo.")] = None,
    nomes_indices: Annotated[
        str | None,
        Query(description="Lista CSV de nomes de índice (ex.: 'PRCPTOT,CDD')."),
    ] = None,
    lat_min: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    lat_max: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    lon_min: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    lon_max: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    raio_km: Annotated[float | None, Query(gt=0.0, description="Exige centro_lat/lon.")] = None,
    centro_lat: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    centro_lon: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    uf: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    municipio_id: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PaginaResultadosResponse:
    filtros = FiltrosResultados(
        execucao_id=execucao_id,
        cenario=cenario,
        variavel=variavel,
        ano=ano,
        ano_min=ano_min,
        ano_max=ano_max,
        nomes_indices=_parse_csv(nomes_indices),
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        raio_km=raio_km,
        centro_lat=centro_lat,
        centro_lon=centro_lon,
        uf=uf.upper() if uf else None,
        municipio_id=municipio_id,
        limit=limit,
        offset=offset,
    )
    pagina = await caso_uso.executar(filtros)
    return _para_pagina_response(pagina)


@router.get(
    "/agregados",
    response_model=AgregacaoResponse,
    status_code=status.HTTP_200_OK,
    summary="Agrega resultados por dimensões (media, min, max, count, p50, p95).",
    responses={
        422: {"model": ProblemDetails, "description": "Combinação inválida de filtros."},
    },
)
async def agregar_resultados(
    caso_uso: AgregarDep,
    agregacao: Annotated[
        str,
        Query(
            description="Função de agregação.",
            pattern="^(media|min|max|count|p50|p95)$",
        ),
    ] = "media",
    agrupar_por: Annotated[
        str | None,
        Query(
            description=(
                "Lista CSV de dimensões (ano, cenario, variavel, "
                "nome_indice, municipio). Vazio = agregação global."
            ),
        ),
    ] = None,
    execucao_id: Annotated[str | None, Query()] = None,
    cenario: Annotated[str | None, Query()] = None,
    variavel: Annotated[str | None, Query()] = None,
    ano: Annotated[int | None, Query()] = None,
    ano_min: Annotated[int | None, Query()] = None,
    ano_max: Annotated[int | None, Query()] = None,
    nomes_indices: Annotated[str | None, Query()] = None,
    lat_min: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    lat_max: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    lon_min: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    lon_max: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    raio_km: Annotated[float | None, Query(gt=0.0)] = None,
    centro_lat: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    centro_lon: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    uf: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    municipio_id: Annotated[int | None, Query(ge=0)] = None,
) -> AgregacaoResponse:
    filtros = FiltrosAgregacao(
        execucao_id=execucao_id,
        cenario=cenario,
        variavel=variavel,
        ano=ano,
        ano_min=ano_min,
        ano_max=ano_max,
        nomes_indices=_parse_csv(nomes_indices),
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        raio_km=raio_km,
        centro_lat=centro_lat,
        centro_lon=centro_lon,
        uf=uf.upper() if uf else None,
        municipio_id=municipio_id,
        agregacao=agregacao,
        agrupar_por=_parse_csv(agrupar_por),
    )
    resultado = await caso_uso.executar(filtros)
    return _para_agregacao_response(resultado)


@router.get(
    "/stats",
    response_model=EstatisticasResponse,
    status_code=status.HTTP_200_OK,
    summary="Estatísticas globais dos resultados armazenados.",
)
async def obter_stats(caso_uso: StatsDep) -> EstatisticasResponse:
    estatisticas = await caso_uso.executar()
    return _para_estatisticas_response(estatisticas)


# ---------------------------------------------------------------------
# Translators domain DTOs -> Pydantic.
# ---------------------------------------------------------------------
def _para_resultado_response(r: ResultadoIndice) -> ResultadoResponse:
    return ResultadoResponse(
        id=r.id,
        execucao_id=r.execucao_id,
        lat=r.lat,
        lon=r.lon,
        lat_input=r.lat_input,
        lon_input=r.lon_input,
        ano=r.ano,
        nome_indice=r.nome_indice,
        valor=r.valor,
        unidade=r.unidade,
        municipio_id=r.municipio_id,
    )


def _para_pagina_response(pagina: PaginaResultados) -> PaginaResultadosResponse:
    return PaginaResultadosResponse(
        total=pagina.total,
        limit=pagina.limit,
        offset=pagina.offset,
        items=[_para_resultado_response(r) for r in pagina.items],
    )


def _para_grupo_response(g: GrupoAgregado) -> GrupoAgregadoResponse:
    return GrupoAgregadoResponse(grupo=g.grupo, valor=g.valor, n_amostras=g.n_amostras)


def _para_agregacao_response(res: ResultadoAgregacao) -> AgregacaoResponse:
    return AgregacaoResponse(
        agregacao=res.agregacao,
        agrupar_por=list(res.agrupar_por),
        grupos=[_para_grupo_response(g) for g in res.grupos],
    )


def _para_estatisticas_response(e: EstatisticasResultados) -> EstatisticasResponse:
    return EstatisticasResponse(
        cenarios=e.cenarios,
        anos=e.anos,
        variaveis=e.variaveis,
        nomes_indices=e.nomes_indices,
        total_execucoes_com_resultados=e.total_execucoes_com_resultados,
        total_resultados=e.total_resultados,
    )
