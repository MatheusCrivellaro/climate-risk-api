"""Rotas REST do pipeline de estresse hídrico (Slice 15).

- ``POST /execucoes/estresse-hidrico`` → cria execução + enfileira job (202).
- ``GET  /resultados/estresse-hidrico`` → lista paginada com filtros.
- ``GET  /resultados/estresse-hidrico/export`` → exporta em CSV/XLSX/JSON
  (Slice 20.1, com limite de 200.000 linhas).

Rotas expostas em dois roteadores separados para que apareçam agrupadas
corretamente no Swagger ("Execuções" e "Resultados").
"""

from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse

from climate_risk.application.indices.calcular_estresse_hidrico import (
    CalcularIndicesEstresseHidrico,
    ExecucaoIniciada,
    ParametrosCalculoEstresseHidrico,
    ParametrosCalculoEstresseHidricoPasta,
)
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
)
from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)
from climate_risk.domain.excecoes import ErroDominio
from climate_risk.infrastructure.db.repositorios.resultado_estresse_hidrico import (
    SQLAlchemyRepositorioResultadoEstresseHidrico,
)
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_calcular_estresse_hidrico,
    obter_repositorio_resultado_estresse_hidrico,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.estresse_hidrico import (
    CenarioPastasSchema,
    CriarExecucaoEstresseHidricoRequest,
    CriarExecucaoEstresseHidricoResponse,
    CriarExecucoesEstresseHidricoEmLoteRequest,
    CriarExecucoesEstresseHidricoEmLoteResponse,
    ItemExecucaoEmLote,
    ListarResultadosEstresseHidricoResponse,
    ResultadoEstresseHidricoSchema,
)

LIMITE_LINHAS_EXPORT = 200_000

CABECALHO_EXPORT: tuple[str, ...] = (
    "id",
    "execucao_id",
    "municipio_id",
    "nome_municipio",
    "uf",
    "ano",
    "cenario",
    "frequencia_dias_secos_quentes",
    "intensidade_mm_dia",
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


@router_execucoes.post(
    "/em-lote",
    response_model=CriarExecucoesEstresseHidricoEmLoteResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary=(
        "Cria duas execuções de estresse hídrico (rcp45 + rcp85) a partir de "
        "pastas de arquivos NetCDF (Slice 17)."
    ),
    responses={
        422: {"model": ProblemDetails, "description": "Erro de validação do corpo."},
    },
)
async def criar_execucoes_estresse_hidrico_em_lote(
    payload: CriarExecucoesEstresseHidricoEmLoteRequest,
    caso_uso: CriarDep,
) -> CriarExecucoesEstresseHidricoEmLoteResponse:
    parametros_indices = ParametrosIndicesEstresseHidrico(
        limiar_pr_mm_dia=payload.parametros.limiar_pr_mm_dia,
        limiar_tas_c=payload.parametros.limiar_tas_c,
    )

    resultados: list[ItemExecucaoEmLote] = []
    for cenario, pastas in (("rcp45", payload.rcp45), ("rcp85", payload.rcp85)):
        item = await _executar_cenario(
            caso_uso=caso_uso,
            cenario=cenario,
            pastas=pastas,
            parametros_indices=parametros_indices,
        )
        resultados.append(item)
    return CriarExecucoesEstresseHidricoEmLoteResponse(execucoes=resultados)


async def _executar_cenario(
    *,
    caso_uso: CalcularIndicesEstresseHidrico,
    cenario: str,
    pastas: CenarioPastasSchema,
    parametros_indices: ParametrosIndicesEstresseHidrico,
) -> ItemExecucaoEmLote:
    """Tenta criar a execução de um cenário; falha não propaga ao outro."""
    params = ParametrosCalculoEstresseHidricoPasta(
        pasta_pr=Path(pastas.pasta_pr),
        pasta_tas=Path(pastas.pasta_tas),
        pasta_evap=Path(pastas.pasta_evap),
        cenario=cenario,
        parametros_indices=parametros_indices,
    )
    try:
        resultado = await caso_uso.executar_de_pasta(params)
    except ErroDominio as exc:
        return ItemExecucaoEmLote(cenario=cenario, erro=str(exc))
    return ItemExecucaoEmLote(
        cenario=cenario,
        execucao_id=resultado.execucao_id,
        job_id=resultado.job_id,
        status=resultado.status,
    )


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
            intensidade_mm_dia=r.intensidade_mm_dia,
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


@router_resultados.get(
    "/export",
    response_model=None,
    summary="Exporta resultados de estresse hídrico em CSV, XLSX ou JSON.",
    responses={
        200: {
            "description": "Arquivo gerado com sucesso.",
            "content": {
                "text/csv": {},
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {},
                "application/json": {},
            },
        },
        400: {
            "model": ProblemDetails,
            "description": "Filtro retorna mais que o limite máximo de linhas.",
        },
    },
)
async def exportar_resultados_estresse_hidrico(
    repo: RepoResultadosDep,
    formato: Annotated[Literal["csv", "xlsx", "json"], Query(...)],
    execucao_id: Annotated[str | None, Query()] = None,
    cenario: Annotated[str | None, Query()] = None,
    ano: Annotated[int | None, Query()] = None,
    ano_min: Annotated[int | None, Query()] = None,
    ano_max: Annotated[int | None, Query()] = None,
    municipio_id: Annotated[int | None, Query(ge=0)] = None,
    uf: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
) -> StreamingResponse | JSONResponse:
    """Exporta resultados filtrados em CSV, XLSX ou JSON.

    Compartilha os mesmos filtros de :func:`listar_resultados_estresse_hidrico`.
    Antes de materializar a saída, conta linhas e aborta com 400 quando
    o filtro produziria mais que :data:`LIMITE_LINHAS_EXPORT` registros —
    proteção contra exports acidentais que estouram memória do servidor.
    """
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
    if total > LIMITE_LINHAS_EXPORT:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Filtro retorna {total} linhas (mais de {LIMITE_LINHAS_EXPORT}). "
                "Use filtros mais específicos."
            ),
        )

    linhas = await repo.listar_com_municipio(
        **filtros,  # type: ignore[arg-type]
        limit=LIMITE_LINHAS_EXPORT,
        offset=0,
    )

    nome_arquivo = _nome_arquivo_export(formato)

    if formato == "csv":
        return _resposta_csv(linhas, nome_arquivo)
    if formato == "xlsx":
        return _resposta_xlsx(linhas, nome_arquivo)
    return _resposta_json(linhas, nome_arquivo)


def _nome_arquivo_export(formato: str) -> str:
    hoje = datetime.now(UTC).date().isoformat()
    return f"resultados_estresse_hidrico_{hoje}.{formato}"


def _linhas_para_dicts(
    linhas: Sequence[tuple[ResultadoEstresseHidrico, str | None, str | None]],
) -> list[dict[str, object]]:
    saida: list[dict[str, object]] = []
    for r, nome, uf_mun in linhas:
        saida.append(
            {
                "id": r.id,
                "execucao_id": r.execucao_id,
                "municipio_id": r.municipio_id,
                "nome_municipio": nome,
                "uf": uf_mun,
                "ano": r.ano,
                "cenario": r.cenario,
                "frequencia_dias_secos_quentes": r.frequencia_dias_secos_quentes,
                "intensidade_mm_dia": r.intensidade_mm_dia,
            }
        )
    return saida


def _resposta_csv(
    linhas: Sequence[tuple[ResultadoEstresseHidrico, str | None, str | None]],
    nome_arquivo: str,
) -> StreamingResponse:
    """Gera CSV em memória com BOM UTF-8 (compatível com Excel BR)."""
    buffer = io.StringIO()
    buffer.write("﻿")
    writer = csv.writer(buffer)
    writer.writerow(CABECALHO_EXPORT)
    for linha in _linhas_para_dicts(linhas):
        writer.writerow([linha[col] if linha[col] is not None else "" for col in CABECALHO_EXPORT])
    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{nome_arquivo}"',
        },
    )


def _resposta_xlsx(
    linhas: Sequence[tuple[ResultadoEstresseHidrico, str | None, str | None]],
    nome_arquivo: str,
) -> StreamingResponse:
    """Gera XLSX via openpyxl com header em negrito e freeze pane."""
    from openpyxl import Workbook
    from openpyxl.styles import Font

    wb = Workbook()
    ws = wb.active
    ws.title = "estresse_hidrico"
    ws.append(list(CABECALHO_EXPORT))
    for celula in ws[1]:
        celula.font = Font(bold=True)
    ws.freeze_panes = "A2"

    for linha in _linhas_para_dicts(linhas):
        ws.append([linha[col] for col in CABECALHO_EXPORT])

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{nome_arquivo}"',
        },
    )


def _resposta_json(
    linhas: Sequence[tuple[ResultadoEstresseHidrico, str | None, str | None]],
    nome_arquivo: str,
) -> JSONResponse:
    """Gera JSON como lista de objetos (UTF-8)."""
    return JSONResponse(
        content=_linhas_para_dicts(linhas),
        headers={
            "Content-Disposition": f'attachment; filename="{nome_arquivo}"',
        },
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
