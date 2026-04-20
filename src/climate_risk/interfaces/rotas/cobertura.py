"""Rota ``POST /cobertura/fornecedores`` (Slice 9).

Substitui o notebook legado ``locais_faltantes_fornecedores.ipynb``
(removido na Slice 12): cruza a lista de fornecedores (estruturada ou
texto legado) com a tabela ``resultado_indice`` para descobrir quem tem
dados climáticos processados.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from climate_risk.application.cobertura import (
    AnalisarCoberturaFornecedores,
    FornecedorEntrada,
    parsear_lista_legacy,
)
from climate_risk.interfaces.dependencias import obter_caso_uso_analisar_cobertura
from climate_risk.interfaces.schemas.cobertura import (
    CoberturaRequest,
    CoberturaResponse,
    FornecedorCoberturaResponse,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails

router = APIRouter(prefix="/cobertura", tags=["cobertura"])

CoberturaDep = Annotated[AnalisarCoberturaFornecedores, Depends(obter_caso_uso_analisar_cobertura)]


@router.post(
    "/fornecedores",
    response_model=CoberturaResponse,
    summary="Identifica quais fornecedores têm dados climáticos processados.",
    responses={
        422: {"model": ProblemDetails, "description": "Erro de validação."},
        503: {"model": ProblemDetails, "description": "API do IBGE indisponível."},
    },
)
async def cobertura_fornecedores(
    payload: CoberturaRequest,
    caso: CoberturaDep,
) -> CoberturaResponse:
    """Recebe fornecedores estruturados ou texto legado e devolve cobertura.

    Formato legado (ex.: conteúdo do ``Localizacao_fornecedores.xlsx``):

    ``MONTE BELO/MG
    CONCEICAO DO MATO DENTRO/MG``

    Cada linha com ``/`` vira um :class:`FornecedorEntrada`; o
    ``identificador`` ecoa a linha original.
    """
    if payload.fornecedores is not None and len(payload.fornecedores) > 0:
        fornecedores = [
            FornecedorEntrada(
                identificador=f.identificador,
                cidade=f.cidade,
                uf=f.uf.upper(),
            )
            for f in payload.fornecedores
        ]
    else:
        # O model_validator garante que texto_legacy está preenchido aqui.
        assert payload.texto_legacy is not None
        fornecedores = parsear_lista_legacy(payload.texto_legacy)

    resultado = await caso.executar(fornecedores)
    return CoberturaResponse(
        total=resultado.total,
        com_cobertura=resultado.com_cobertura,
        sem_cobertura=resultado.sem_cobertura,
        itens=[
            FornecedorCoberturaResponse(
                identificador=item.identificador,
                cidade_entrada=item.cidade_entrada,
                uf_entrada=item.uf_entrada,
                tem_cobertura=item.tem_cobertura,
                municipio_id=item.municipio_id,
                nome_canonico=item.nome_canonico,
                motivo_nao_encontrado=item.motivo_nao_encontrado,
            )
            for item in resultado.itens
        ],
    )
