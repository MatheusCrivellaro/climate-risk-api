"""Rotas administrativas (Slice 8).

Por enquanto apenas ``POST /admin/ibge/refresh`` — repovoa o cache de
municípios a partir da API do IBGE. Fica isolado em ``/admin`` para
permitir proteção futura por autenticação sem afetar o domínio.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from climate_risk.application.geocodificacao import RefreshCatalogoIBGE
from climate_risk.interfaces.dependencias import obter_caso_uso_refresh_ibge
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.geocoding import RefreshIBGEResponse

router = APIRouter(prefix="/admin", tags=["admin"])

RefreshDep = Annotated[RefreshCatalogoIBGE, Depends(obter_caso_uso_refresh_ibge)]


@router.post(
    "/ibge/refresh",
    response_model=RefreshIBGEResponse,
    summary="Recarrega o cache de municípios a partir da API do IBGE.",
    responses={
        503: {"model": ProblemDetails, "description": "API do IBGE indisponível."},
    },
)
async def refresh_ibge(caso: RefreshDep) -> RefreshIBGEResponse:
    """Baixa o catálogo completo (~5570 municípios) e atualiza o cache.

    Operação cara — uso típico é esporádico (provisionamento, upgrade
    anual do IBGE). Nenhuma autenticação por enquanto: o endpoint ``/admin``
    serve de placeholder para uma camada futura.
    """
    resultado = await caso.executar()
    return RefreshIBGEResponse(
        total_municipios=resultado.total_municipios,
        com_centroide=resultado.com_centroide,
        sem_centroide=resultado.sem_centroide,
    )
