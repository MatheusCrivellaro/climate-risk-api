"""Testes unitários de :class:`CriarFornecedor` (Slice 10)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.fornecedores import (
    CriarFornecedor,
    ParametrosCriacaoFornecedor,
)
from climate_risk.domain.entidades.fornecedor import Fornecedor


@dataclass
class _RepoFake:
    salvos: list[Fornecedor] = field(default_factory=list)

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor | None:
        for f in self.salvos:
            if f.id == fornecedor_id:
                return f
        return None

    async def buscar_por_nome_cidade_uf(self, nome: str, cidade: str, uf: str) -> Fornecedor | None:
        for f in self.salvos:
            if f.nome == nome and f.cidade == cidade and f.uf == uf:
                return f
        return None

    async def salvar(self, fornecedor: Fornecedor) -> None:
        self.salvos.append(fornecedor)

    async def salvar_lote(self, fornecedores: Sequence[Fornecedor]) -> None:
        self.salvos.extend(fornecedores)

    async def listar(self, **_: object) -> list[Fornecedor]:
        return list(self.salvos)

    async def contar(self, **_: object) -> int:
        return len(self.salvos)

    async def remover(self, fornecedor_id: str) -> bool:
        antes = len(self.salvos)
        self.salvos = [f for f in self.salvos if f.id != fornecedor_id]
        return len(self.salvos) < antes


@pytest.mark.asyncio
async def test_criar_gera_id_e_preenche_timestamps() -> None:
    repo = _RepoFake()
    caso = CriarFornecedor(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        ParametrosCriacaoFornecedor(nome="Acme", cidade="São Paulo", uf="SP")
    )

    assert resultado.id.startswith("forn_")
    assert resultado.nome == "Acme"
    assert resultado.cidade == "São Paulo"
    assert resultado.uf == "SP"
    assert resultado.criado_em == resultado.atualizado_em
    assert resultado.municipio_id is None
    assert resultado.lat is None and resultado.lon is None
    assert len(repo.salvos) == 1


@pytest.mark.asyncio
async def test_criar_aceita_identificador_externo_e_municipio_id() -> None:
    repo = _RepoFake()
    caso = CriarFornecedor(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        ParametrosCriacaoFornecedor(
            nome="Beta",
            cidade="Rio de Janeiro",
            uf="RJ",
            identificador_externo="ext-42",
            municipio_id=3304557,
        )
    )

    assert resultado.identificador_externo == "ext-42"
    assert resultado.municipio_id == 3304557
