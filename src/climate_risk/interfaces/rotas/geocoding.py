"""Rotas de ``/localizacoes`` (Slices 8 e 9).

- Slice 8: ``POST /localizacoes/geocodificar`` (CIDADE/UF → lat/lon).
- Slice 9: ``POST /localizacoes/localizar`` (lat/lon → município/UF).

``POST /admin/ibge/refresh`` fica em ``rotas/admin.py``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from climate_risk.application.geocodificacao import (
    EntradaLocalizacao,
    GeocodificarLocalizacoes,
)
from climate_risk.application.localizacoes import (
    LocalizarPontos,
    PontoParaLocalizar,
)
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_geocodificar,
    obter_caso_uso_localizar_pontos,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.geocoding import (
    GeocodificarRequest,
    GeocodificarResponse,
    LocalizacaoGeocodificadaSchema,
)
from climate_risk.interfaces.schemas.localizacoes import (
    LocalizarPontosRequest,
    LocalizarPontosResponse,
    PontoLocalizadoResponse,
)

router = APIRouter(prefix="/localizacoes", tags=["geocodificacao"])

GeocodificarDep = Annotated[GeocodificarLocalizacoes, Depends(obter_caso_uso_geocodificar)]
LocalizarDep = Annotated[LocalizarPontos, Depends(obter_caso_uso_localizar_pontos)]


@router.post(
    "/geocodificar",
    response_model=GeocodificarResponse,
    summary="Geocodifica pares (cidade, UF) usando cache local + IBGE.",
    responses={
        422: {"model": ProblemDetails, "description": "Erro de validação."},
        503: {"model": ProblemDetails, "description": "API do IBGE indisponível."},
    },
)
async def geocodificar(
    payload: GeocodificarRequest,
    caso: GeocodificarDep,
) -> GeocodificarResponse:
    """Resolve cada ``(cidade, uf)`` via cache → fuzzy → IBGE.

    Entradas não encontradas não bloqueiam o lote — aparecem com
    ``metodo="nao_encontrado"``. Se a API do IBGE falhar enquanto uma UF
    ainda não foi cacheada, os itens daquela UF voltam com
    ``metodo="api_falhou"`` e o endpoint ainda responde 200.
    """
    entradas = [EntradaLocalizacao(cidade=p.cidade, uf=p.uf.upper()) for p in payload.localizacoes]
    resultado = await caso.executar(entradas)
    return GeocodificarResponse(
        total=resultado.total,
        encontrados=resultado.encontrados,
        nao_encontrados=resultado.nao_encontrados,
        itens=[
            LocalizacaoGeocodificadaSchema(
                cidade_entrada=i.cidade_entrada,
                uf=i.uf,
                municipio_id=i.municipio_id,
                nome_canonico=i.nome_canonico,
                lat=i.lat,
                lon=i.lon,
                metodo=i.metodo,
            )
            for i in resultado.itens
        ],
    )


@router.post(
    "/localizar",
    response_model=LocalizarPontosResponse,
    summary="Resolve pares (lat, lon) em município/UF via shapefile.",
    responses={
        422: {"model": ProblemDetails, "description": "Erro de validação."},
        500: {"model": ProblemDetails, "description": "Shapefile não configurado."},
    },
)
async def localizar(
    payload: LocalizarPontosRequest,
    caso: LocalizarDep,
) -> LocalizarPontosResponse:
    """Point-in-polygon sobre a malha de municípios carregada em memória.

    Pontos fora do território brasileiro voltam com ``encontrado=false``
    e campos de município nulos — o endpoint não bloqueia o lote.
    """
    pontos = [
        PontoParaLocalizar(lat=p.lat, lon=p.lon, identificador=p.identificador)
        for p in payload.pontos
    ]
    resultado = await caso.executar(pontos)
    return LocalizarPontosResponse(
        total=resultado.total,
        encontrados=resultado.encontrados,
        itens=[
            PontoLocalizadoResponse(
                lat=item.lat,
                lon=item.lon,
                identificador=item.identificador,
                encontrado=item.encontrado,
                municipio_id=item.municipio_id,
                uf=item.uf,
                nome_municipio=item.nome_municipio,
            )
            for item in resultado.itens
        ],
    )
