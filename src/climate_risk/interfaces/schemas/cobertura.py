"""Schemas Pydantic de ``POST /cobertura/fornecedores`` (Slice 9)."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class FornecedorEntradaRequest(BaseModel):
    """Fornecedor em formato estruturado."""

    identificador: str = Field(..., min_length=1, max_length=120)
    cidade: str = Field(..., min_length=1, max_length=120)
    uf: str = Field(..., min_length=2, max_length=2)


class CoberturaRequest(BaseModel):
    """Corpo da requisição — aceita formato estruturado OU texto legado.

    Exatamente um dos dois campos deve vir preenchido; ambos ou nenhum
    resultam em ``422``. O texto legado segue o formato de
    ``locais_faltantes_fornecedores.ipynb``: uma linha por fornecedor,
    ``CIDADE/UF``, linhas em branco ou sem ``/`` ignoradas.
    """

    fornecedores: list[FornecedorEntradaRequest] | None = Field(
        default=None,
        description="Lista estruturada; mutuamente exclusiva com 'texto_legacy'.",
    )
    texto_legacy: str | None = Field(
        default=None,
        description="Texto bruto no formato CIDADE/UF por linha.",
    )

    @model_validator(mode="after")
    def _validar_um_dos_dois(self) -> CoberturaRequest:
        tem_lista = self.fornecedores is not None and len(self.fornecedores) > 0
        tem_texto = self.texto_legacy is not None and self.texto_legacy.strip() != ""
        if tem_lista == tem_texto:
            raise ValueError("forneça 'fornecedores' ou 'texto_legacy' (exatamente um dos dois).")
        return self


class FornecedorCoberturaResponse(BaseModel):
    """Resposta unitária."""

    identificador: str
    cidade_entrada: str
    uf_entrada: str
    tem_cobertura: bool
    municipio_id: int | None = None
    nome_canonico: str | None = None
    motivo_nao_encontrado: str | None = Field(
        default=None,
        description=(
            "Quando tem_cobertura=False: 'municipio_nao_geocodificado' ou 'sem_dados_climaticos'."
        ),
    )


class CoberturaResponse(BaseModel):
    """Resposta agregada."""

    total: int = Field(..., ge=0)
    com_cobertura: int = Field(..., ge=0)
    sem_cobertura: int = Field(..., ge=0)
    itens: list[FornecedorCoberturaResponse]
