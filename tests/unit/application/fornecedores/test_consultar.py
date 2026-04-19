"""Testes unitários de :class:`ConsultarFornecedores` (Slice 10)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from climate_risk.application.fornecedores import (
    ConsultarFornecedores,
    FiltrosConsultaFornecedores,
)
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada


def _forn(id_: str, nome: str = "X", cidade: str = "São Paulo", uf: str = "SP") -> Fornecedor:
    agora = datetime(2026, 4, 16, tzinfo=UTC)
    return Fornecedor(id=id_, nome=nome, cidade=cidade, uf=uf, criado_em=agora, atualizado_em=agora)


@dataclass
class _RepoFake:
    dados: list[Fornecedor] = field(default_factory=list)

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor | None:
        for f in self.dados:
            if f.id == fornecedor_id:
                return f
        return None

    async def buscar_por_nome_cidade_uf(self, *args: object) -> Fornecedor | None:
        return None

    async def salvar(self, fornecedor: Fornecedor) -> None:
        self.dados.append(fornecedor)

    async def salvar_lote(self, fornecedores: Sequence[Fornecedor]) -> None:
        self.dados.extend(fornecedores)

    async def listar(
        self,
        uf: str | None = None,
        cidade: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Fornecedor]:
        itens = [
            f
            for f in self.dados
            if (uf is None or f.uf == uf) and (cidade is None or f.cidade == cidade)
        ]
        return itens[offset : offset + limit]

    async def contar(self, uf: str | None = None, cidade: str | None = None) -> int:
        return len(
            [
                f
                for f in self.dados
                if (uf is None or f.uf == uf) and (cidade is None or f.cidade == cidade)
            ]
        )

    async def remover(self, fornecedor_id: str) -> bool:
        return False


@pytest.mark.asyncio
async def test_listar_pagina_aplica_filtros() -> None:
    repo = _RepoFake(
        dados=[
            _forn("forn_1", nome="a", uf="SP", cidade="São Paulo"),
            _forn("forn_2", nome="b", uf="SP", cidade="Campinas"),
            _forn("forn_3", nome="c", uf="RJ", cidade="Rio de Janeiro"),
        ]
    )
    caso = ConsultarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.listar(FiltrosConsultaFornecedores(uf="SP", limit=10, offset=0))

    assert pagina.total == 2
    assert pagina.limit == 10
    assert pagina.offset == 0
    assert {f.id for f in pagina.itens} == {"forn_1", "forn_2"}


@pytest.mark.asyncio
async def test_listar_sem_filtros_devolve_todos() -> None:
    repo = _RepoFake(dados=[_forn(f"forn_{i}") for i in range(3)])
    caso = ConsultarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.listar(FiltrosConsultaFornecedores())

    assert pagina.total == 3
    assert len(pagina.itens) == 3


@pytest.mark.asyncio
async def test_buscar_por_id_existente() -> None:
    repo = _RepoFake(dados=[_forn("forn_abc")])
    caso = ConsultarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    f = await caso.buscar_por_id("forn_abc")
    assert f.id == "forn_abc"


@pytest.mark.asyncio
async def test_buscar_por_id_nao_encontrado_levanta() -> None:
    repo = _RepoFake()
    caso = ConsultarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.buscar_por_id("forn_xxx")
