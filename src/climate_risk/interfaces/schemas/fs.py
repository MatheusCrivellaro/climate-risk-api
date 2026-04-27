"""Schemas Pydantic do browser de pastas (Slice 20.1)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ItemPasta(BaseModel):
    """Subpasta listada por ``GET /api/fs/listar``."""

    nome: str = Field(..., description="Nome da pasta (basename).")
    caminho_absoluto: str = Field(..., description="Caminho absoluto resolvido.")
    quantidade_nc: int = Field(
        ..., ge=0, description="Quantidade de arquivos .nc diretamente dentro da pasta."
    )


class ItemArquivo(BaseModel):
    """Arquivo ``.nc`` listado por ``GET /api/fs/listar``."""

    nome: str = Field(..., description="Nome do arquivo (basename).")
    tamanho_bytes: int = Field(..., ge=0)
    cenario_detectado: str | None = Field(
        default=None,
        description="Cenário CORDEX inferido do nome do arquivo (ex.: 'rcp45').",
    )


class ListarPastaResponse(BaseModel):
    """Corpo da resposta de ``GET /api/fs/listar``."""

    caminho_atual: str
    caminho_relativo_raiz: str
    pasta_raiz: str
    pode_subir: bool
    pasta_pai: str | None = None
    subpastas: list[ItemPasta] = Field(default_factory=list)
    arquivos_nc: list[ItemArquivo] = Field(default_factory=list)
