"""Rotas REST do browser de pastas (Slice 20.1).

Expõe ``GET /api/fs/listar``, que devolve subpastas e arquivos ``.nc`` de
um diretório autorizado pelo servidor. Existe para alimentar o seletor
visual de pastas da página ``/estudo/`` — usuários precisam apontar o
backend para 6 pastas (``pr``/``tas``/``evap`` x ``rcp45``/``rcp85``).

A pasta raiz é definida pela variável de ambiente ``CLIMATE_RISK_FS_RAIZ``
(:class:`ConfigFS`). Sem ela, o endpoint responde 503 — não há fallback
inseguro para ``/`` ou para o cwd. Toda navegação é feita relativa à raiz
e qualquer tentativa de sair dela (``..`` ou symlink apontando para fora)
é bloqueada via :func:`Path.resolve(strict=True)` + :func:`Path.relative_to`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from climate_risk.core.config import ConfigFS
from climate_risk.infrastructure.leitor_cordex_multi import detectar_cenario_no_nome
from climate_risk.interfaces.schemas.comum import ProblemDetails
from climate_risk.interfaces.schemas.fs import (
    ItemArquivo,
    ItemPasta,
    ListarPastaResponse,
)

router = APIRouter(prefix="/fs", tags=["fs"])


def obter_config_fs() -> ConfigFS:
    """Provider de :class:`ConfigFS` para FastAPI.

    Recarregado a cada chamada para refletir mudanças em
    ``CLIMATE_RISK_FS_RAIZ`` em testes que usam ``monkeypatch.setenv``.
    """
    return ConfigFS.from_env()


ConfigFSDep = Annotated[ConfigFS, Depends(obter_config_fs)]


def _validar_caminho(caminho_solicitado: str, raiz: Path) -> Path:
    """Resolve ``caminho_solicitado`` e garante que está dentro de ``raiz``.

    A ordem aqui é deliberada: ``resolve(strict=True)`` ANTES de
    ``relative_to(raiz)``. Resolver primeiro força o sistema operacional
    a seguir ``..`` e symlinks até o caminho final real; comparar com a
    raiz depois detecta tentativas tipo ``raiz/../etc/passwd`` ou symlinks
    apontando para fora. Inverter a ordem (validar antes de resolver)
    deixaria passar ``raiz/link-para-root`` quando ``link-para-root`` é
    um symlink para ``/``.
    """
    try:
        caminho_resolvido = Path(caminho_solicitado).resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Caminho não existe: {exc}",
        ) from exc

    try:
        caminho_resolvido.relative_to(raiz)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Caminho está fora da pasta raiz autorizada.",
        ) from exc

    if not caminho_resolvido.is_dir():
        raise HTTPException(
            status_code=400,
            detail="Caminho não é diretório.",
        )

    return caminho_resolvido


def _contar_nc(pasta: Path) -> int:
    """Conta arquivos ``.nc`` diretamente dentro de ``pasta`` (não recursivo)."""
    try:
        return sum(1 for entry in pasta.iterdir() if entry.is_file() and entry.suffix == ".nc")
    except (PermissionError, OSError):
        return 0


def _listar_subpastas(pasta: Path) -> list[ItemPasta]:
    items: list[ItemPasta] = []
    for entry in sorted(pasta.iterdir(), key=lambda p: p.name.lower()):
        if entry.is_dir():
            items.append(
                ItemPasta(
                    nome=entry.name,
                    caminho_absoluto=str(entry.resolve()),
                    quantidade_nc=_contar_nc(entry),
                )
            )
    return items


def _listar_arquivos_nc(pasta: Path) -> list[ItemArquivo]:
    items: list[ItemArquivo] = []
    for entry in sorted(pasta.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_file() or entry.suffix != ".nc":
            continue
        try:
            tamanho = entry.stat().st_size
        except OSError:
            tamanho = 0
        items.append(
            ItemArquivo(
                nome=entry.name,
                tamanho_bytes=tamanho,
                cenario_detectado=detectar_cenario_no_nome(entry.name),
            )
        )
    return items


@router.get(
    "/listar",
    response_model=ListarPastaResponse,
    summary="Lista subpastas e arquivos .nc de um diretório do servidor.",
    responses={
        400: {
            "model": ProblemDetails,
            "description": "Caminho inválido ou fora da raiz autorizada.",
        },
        404: {"model": ProblemDetails, "description": "Caminho não existe."},
        503: {
            "model": ProblemDetails,
            "description": "Browser de pastas não configurado (CLIMATE_RISK_FS_RAIZ ausente).",
        },
    },
)
async def listar_pasta(
    config_fs: ConfigFSDep,
    caminho: Annotated[
        str | None,
        Query(
            description=(
                "Caminho absoluto do diretório a listar. Deve estar dentro de "
                "CLIMATE_RISK_FS_RAIZ. Sem este parâmetro, lista a raiz."
            ),
        ),
    ] = None,
) -> ListarPastaResponse:
    """Lista subpastas e arquivos ``.nc`` de um diretório autorizado.

    - Sem ``caminho``: lista a partir de ``CLIMATE_RISK_FS_RAIZ``.
    - Com ``caminho``: deve estar dentro da raiz (path traversal proibido).
    - 503 quando a env var não está setada.
    - 400 quando ``caminho`` aponta para fora da raiz ou não é diretório.
    - 404 quando ``caminho`` não existe.
    """
    if config_fs.raiz is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Browser de pastas não configurado. Defina CLIMATE_RISK_FS_RAIZ "
                "para a pasta raiz autorizada e reinicie o servidor."
            ),
        )

    raiz = config_fs.raiz
    alvo = raiz if caminho is None else _validar_caminho(caminho, raiz)

    relativo = alvo.relative_to(raiz)
    pode_subir = alvo != raiz
    pasta_pai = str(alvo.parent.resolve()) if pode_subir else None

    return ListarPastaResponse(
        caminho_atual=str(alvo),
        caminho_relativo_raiz=str(relativo),
        pasta_raiz=str(raiz),
        pode_subir=pode_subir,
        pasta_pai=pasta_pai,
        subpastas=_listar_subpastas(alvo),
        arquivos_nc=_listar_arquivos_nc(alvo),
    )
