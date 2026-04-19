"""Testes unitários de :class:`RemoverFornecedor` (Slice 10)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.fornecedores import RemoverFornecedor
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada


@dataclass
class _RepoFake:
    removidos: list[str] = field(default_factory=list)
    retorno: bool = True

    async def buscar_por_id(self, *a: object) -> Fornecedor | None:
        return None

    async def buscar_por_nome_cidade_uf(self, *a: object) -> Fornecedor | None:
        return None

    async def salvar(self, fornecedor: Fornecedor) -> None:
        pass

    async def salvar_lote(self, fornecedores: Sequence[Fornecedor]) -> None:
        pass

    async def listar(self, **_: object) -> list[Fornecedor]:
        return []

    async def contar(self, **_: object) -> int:
        return 0

    async def remover(self, fornecedor_id: str) -> bool:
        self.removidos.append(fornecedor_id)
        return self.retorno


@pytest.mark.asyncio
async def test_remover_existente_ok() -> None:
    repo = _RepoFake(retorno=True)
    caso = RemoverFornecedor(repositorio=repo)  # type: ignore[arg-type]

    await caso.executar("forn_1")
    assert repo.removidos == ["forn_1"]


@pytest.mark.asyncio
async def test_remover_inexistente_levanta_404() -> None:
    repo = _RepoFake(retorno=False)
    caso = RemoverFornecedor(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.executar("forn_xxx")
