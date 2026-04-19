"""Rotas REST para fornecedores (``/fornecedores``) — Slice 10.

Expõe CRUD completo + import em lote via CSV/XLSX:

- ``POST /fornecedores``          → cria (201).
- ``GET  /fornecedores``          → lista paginada.
- ``GET  /fornecedores/{id}``     → detalhe.
- ``DELETE /fornecedores/{id}``   → remove (204).
- ``POST /fornecedores/importar`` → import em lote via CSV ou XLSX.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, File, Query, Response, UploadFile, status

from climate_risk.application.fornecedores import (
    ConsultarFornecedores,
    CriarFornecedor,
    FiltrosConsultaFornecedores,
    ImportarFornecedores,
    LinhaImportacao,
    ParametrosCriacaoFornecedor,
    RemoverFornecedor,
    ResultadoImportacao,
)
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroFormatoInvalido
from climate_risk.infrastructure.importers import (
    LinhaImportacaoBruta,
    ler_fornecedores_csv,
    ler_fornecedores_xlsx,
)
from climate_risk.interfaces.dependencias import (
    obter_caso_uso_consultar_fornecedores,
    obter_caso_uso_criar_fornecedor,
    obter_caso_uso_importar_fornecedores,
    obter_caso_uso_remover_fornecedor,
)
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.fornecedores import (
    ErroLinhaResponse,
    FornecedorRequest,
    FornecedorResponse,
    PaginaFornecedoresResponse,
    ResultadoImportacaoResponse,
)

router = APIRouter(prefix="/fornecedores", tags=["fornecedores"])

CriarDep = Annotated[CriarFornecedor, Depends(obter_caso_uso_criar_fornecedor)]
ConsultarDep = Annotated[ConsultarFornecedores, Depends(obter_caso_uso_consultar_fornecedores)]
RemoverDep = Annotated[RemoverFornecedor, Depends(obter_caso_uso_remover_fornecedor)]
ImportarDep = Annotated[ImportarFornecedores, Depends(obter_caso_uso_importar_fornecedores)]


@router.post(
    "",
    response_model=FornecedorResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cadastra um fornecedor.",
    responses={
        409: {"model": ProblemDetails, "description": "Conflito de integridade."},
        422: {"model": ProblemDetails, "description": "Erro de validação do corpo."},
    },
)
async def criar_fornecedor(payload: FornecedorRequest, caso_uso: CriarDep) -> FornecedorResponse:
    params = ParametrosCriacaoFornecedor(
        nome=payload.nome,
        cidade=payload.cidade,
        uf=payload.uf.upper(),
        identificador_externo=payload.identificador_externo,
        municipio_id=payload.municipio_id,
    )
    fornecedor = await caso_uso.executar(params)
    return _para_response(fornecedor)


@router.get(
    "",
    response_model=PaginaFornecedoresResponse,
    summary="Lista fornecedores com filtros opcionais por UF e cidade.",
)
async def listar_fornecedores(
    caso_uso: ConsultarDep,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cidade: str | None = Query(default=None, min_length=1, max_length=120),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> PaginaFornecedoresResponse:
    filtros = FiltrosConsultaFornecedores(
        uf=uf.upper() if uf else None,
        cidade=cidade,
        limit=limit,
        offset=offset,
    )
    pagina = await caso_uso.listar(filtros)
    return PaginaFornecedoresResponse(
        total=pagina.total,
        limit=pagina.limit,
        offset=pagina.offset,
        itens=[_para_response(f) for f in pagina.itens],
    )


@router.get(
    "/{fornecedor_id}",
    response_model=FornecedorResponse,
    summary="Detalhe de um fornecedor.",
    responses={404: {"model": ProblemDetails, "description": "Fornecedor não encontrado."}},
)
async def obter_fornecedor(fornecedor_id: str, caso_uso: ConsultarDep) -> FornecedorResponse:
    fornecedor = await caso_uso.buscar_por_id(fornecedor_id)
    return _para_response(fornecedor)


@router.delete(
    "/{fornecedor_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove um fornecedor.",
    responses={404: {"model": ProblemDetails, "description": "Fornecedor não encontrado."}},
)
async def remover_fornecedor(fornecedor_id: str, caso_uso: RemoverDep) -> Response:
    await caso_uso.executar(fornecedor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/importar",
    response_model=ResultadoImportacaoResponse,
    summary="Importa fornecedores em lote via arquivo CSV ou XLSX.",
    responses={
        400: {"model": ProblemDetails, "description": "Formato de arquivo não suportado."},
    },
)
async def importar_fornecedores(
    caso_uso: ImportarDep,
    arquivo: Annotated[UploadFile, File(...)],
) -> ResultadoImportacaoResponse:
    """Detecta o formato pela extensão do arquivo.

    Linhas sem ``nome``/``cidade``/``uf`` viram erros relatados; duplicatas
    (mesmo ``nome+cidade+uf``, existente ou dentro do próprio lote) são
    ignoradas. Não faz geocodificação — use ``POST /localizacoes/geocodificar``
    depois se precisar preencher coordenadas.
    """
    nome_arquivo = (arquivo.filename or "").lower()
    conteudo = await arquivo.read()

    if nome_arquivo.endswith(".csv"):
        linhas_brutas = ler_fornecedores_csv(conteudo)
    elif nome_arquivo.endswith(".xlsx"):
        linhas_brutas = ler_fornecedores_xlsx(conteudo)
    else:
        raise ErroFormatoInvalido(
            f"formato de '{arquivo.filename or ''}' não suportado — use .csv ou .xlsx."
        )

    linhas = [_converter_linha(bruta) for bruta in linhas_brutas]
    resultado = await caso_uso.executar(linhas)
    return _para_response_importacao(resultado)


# ---------------------------------------------------------------------
# Translators.
# ---------------------------------------------------------------------
def _para_response(fornecedor: Fornecedor) -> FornecedorResponse:
    return FornecedorResponse(
        id=fornecedor.id,
        nome=fornecedor.nome,
        cidade=fornecedor.cidade,
        uf=fornecedor.uf,
        identificador_externo=fornecedor.identificador_externo,
        lat=fornecedor.lat,
        lon=fornecedor.lon,
        municipio_id=fornecedor.municipio_id,
        criado_em=fornecedor.criado_em.isoformat(),
        atualizado_em=fornecedor.atualizado_em.isoformat(),
    )


def _converter_linha(bruta: LinhaImportacaoBruta) -> LinhaImportacao:
    return LinhaImportacao(
        nome=bruta.nome,
        cidade=bruta.cidade,
        uf=bruta.uf,
        identificador_linha=bruta.numero_linha,
    )


def _para_response_importacao(resultado: ResultadoImportacao) -> ResultadoImportacaoResponse:
    return ResultadoImportacaoResponse(
        total_linhas=resultado.total_linhas,
        importados=resultado.importados,
        duplicados=resultado.duplicados,
        erros=[ErroLinhaResponse(linha=e.linha, motivo=e.motivo) for e in resultado.erros],
    )
