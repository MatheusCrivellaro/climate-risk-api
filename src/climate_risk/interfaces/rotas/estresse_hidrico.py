"""Rotas REST do pipeline de estresse hídrico (Slice 15).

- ``POST /execucoes/estresse-hidrico`` → cria execução + enfileira job (202).
- ``GET  /resultados/estresse-hidrico`` → lista paginada com filtros.

Rotas expostas em dois roteadores separados para que apareçam agrupadas
corretamente no Swagger ("Execuções" e "Resultados").
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from climate_risk.application.indices.calcular_estresse_hidrico import (
    CalcularIndicesEstresseHidrico,
    ExecucaoIniciada,
    ParametrosCalculoEstresseHidrico,
)
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
)
from climate_risk.infrastructure.db.repositorios.resultado_estresse_hidrico import (
    SQLAlchemyRepositorioResultadoEstresseHidrico,
)
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_calcular_estresse_hidrico,
    obter_repositorio_resultado_estresse_hidrico,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.estresse_hidrico import (
    CriarExecucaoEstresseHidricoRequest,
    CriarExecucaoEstresseHidricoResponse,
    ListarResultadosEstresseHidricoResponse,
    ResultadoEstresseHidricoSchema,
)

router_execucoes = APIRouter(
    prefix="/execucoes/estresse-hidrico",
    tags=["execucoes"],
)
router_resultados = APIRouter(
    prefix="/resultados/estresse-hidrico",
    tags=["resultados"],
)


CriarDep = Annotated[
    CalcularIndicesEstresseHidrico,
    Depends(obter_caso_uso_calcular_estresse_hidrico),
]
RepoResultadosDep = Annotated[
    SQLAlchemyRepositorioResultadoEstresseHidrico,
    Depends(obter_repositorio_resultado_estresse_hidrico),
]


@router_execucoes.post(
    "",
    response_model=CriarExecucaoEstresseHidricoResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Cria uma execução de estresse hídrico e enfileira o job de processamento.",
    responses={
        404: {
            "model": ProblemDetails,
            "description": "Algum dos três arquivos NetCDF não foi encontrado.",
        },
        422: {"model": ProblemDetails, "description": "Erro de validação do corpo."},
    },
)
async def criar_execucao_estresse_hidrico(
    payload: CriarExecucaoEstresseHidricoRequest,
    caso_uso: CriarDep,
) -> CriarExecucaoEstresseHidricoResponse:
    params = ParametrosCalculoEstresseHidrico(
        arquivo_pr=Path(payload.arquivo_pr),
        arquivo_tas=Path(payload.arquivo_tas),
        arquivo_evap=Path(payload.arquivo_evap),
        cenario=payload.cenario,
        parametros_indices=ParametrosIndicesEstresseHidrico(
            limiar_pr_mm_dia=payload.parametros.limiar_pr_mm_dia,
            limiar_tas_c=payload.parametros.limiar_tas_c,
        ),
    )
    resultado = await caso_uso.executar(params)
    return _traduzir_execucao_iniciada(resultado)


@router_resultados.get(
    "",
    response_model=ListarResultadosEstresseHidricoResponse,
    status_code=status.HTTP_200_OK,
    summary="Lista resultados de estresse hídrico com filtros opcionais.",
)
async def listar_resultados_estresse_hidrico(
    repo: RepoResultadosDep,
    execucao_id: Annotated[str | None, Query()] = None,
    cenario: Annotated[str | None, Query()] = None,
    ano: Annotated[int | None, Query()] = None,
    ano_min: Annotated[int | None, Query()] = None,
    ano_max: Annotated[int | None, Query()] = None,
    municipio_id: Annotated[int | None, Query(ge=0)] = None,
    uf: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ListarResultadosEstresseHidricoResponse:
    uf_normalizada = uf.upper() if uf else None
    filtros: dict[str, object] = {
        "execucao_id": execucao_id,
        "cenario": cenario,
        "ano": ano,
        "ano_min": ano_min,
        "ano_max": ano_max,
        "municipio_id": municipio_id,
        "uf": uf_normalizada,
    }
    total = await repo.contar(**filtros)  # type: ignore[arg-type]
    linhas = await repo.listar_com_municipio(
        **filtros,  # type: ignore[arg-type]
        limit=limit,
        offset=offset,
    )
    items = [
        ResultadoEstresseHidricoSchema(
            id=r.id,
            execucao_id=r.execucao_id,
            municipio_id=r.municipio_id,
            ano=r.ano,
            cenario=r.cenario,
            frequencia_dias_secos_quentes=r.frequencia_dias_secos_quentes,
            intensidade_mm=r.intensidade_mm,
            nome_municipio=nome,
            uf=uf_mun,
        )
        for r, nome, uf_mun in linhas
    ]
    return ListarResultadosEstresseHidricoResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )


def _traduzir_execucao_iniciada(
    resultado: ExecucaoIniciada,
) -> CriarExecucaoEstresseHidricoResponse:
    return CriarExecucaoEstresseHidricoResponse(
        execucao_id=resultado.execucao_id,
        job_id=resultado.job_id,
        status=resultado.status,
        criado_em=resultado.criado_em.isoformat(),
        links={
            "self": f"/api/execucoes/{resultado.execucao_id}",
            "job": f"/api/jobs/{resultado.job_id}",
        },
    )
