"""Testes unitários de :class:`ImportarFornecedores` (Slice 10)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from climate_risk.application.fornecedores import (
    ImportarFornecedores,
    LinhaImportacao,
)
from climate_risk.domain.entidades.fornecedor import Fornecedor


@dataclass
class _RepoFake:
    existentes: list[tuple[str, str, str]] = field(default_factory=list)
    persistidos: list[Fornecedor] = field(default_factory=list)
    lotes: list[list[Fornecedor]] = field(default_factory=list)

    async def buscar_por_id(self, *a: object) -> Fornecedor | None:
        return None

    async def buscar_por_nome_cidade_uf(self, nome: str, cidade: str, uf: str) -> Fornecedor | None:
        if (nome, cidade, uf) in self.existentes:
            agora = datetime(2026, 4, 16, tzinfo=UTC)
            return Fornecedor(
                id="forn_existing",
                nome=nome,
                cidade=cidade,
                uf=uf,
                criado_em=agora,
                atualizado_em=agora,
            )
        return None

    async def salvar(self, fornecedor: Fornecedor) -> None:
        self.persistidos.append(fornecedor)

    async def salvar_lote(self, fornecedores: Sequence[Fornecedor]) -> None:
        lista = list(fornecedores)
        self.persistidos.extend(lista)
        self.lotes.append(lista)

    async def listar(self, **_: object) -> list[Fornecedor]:
        return list(self.persistidos)

    async def contar(self, **_: object) -> int:
        return len(self.persistidos)

    async def remover(self, fornecedor_id: str) -> bool:
        return False


@pytest.mark.asyncio
async def test_importa_linhas_validas() -> None:
    repo = _RepoFake()
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    linhas = [
        LinhaImportacao(nome="Acme", cidade="São Paulo", uf="sp", identificador_linha=2),
        LinhaImportacao(nome="Beta", cidade="Rio", uf="RJ", identificador_linha=3),
    ]
    resultado = await caso.executar(linhas)

    assert resultado.total_linhas == 2
    assert resultado.importados == 2
    assert resultado.duplicados == 0
    assert resultado.erros == []
    assert len(repo.persistidos) == 2
    # Normaliza UF para uppercase.
    assert {f.uf for f in repo.persistidos} == {"SP", "RJ"}


@pytest.mark.asyncio
async def test_linhas_invalidas_viram_erros() -> None:
    repo = _RepoFake()
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    linhas = [
        LinhaImportacao(nome="  ", cidade="SP", uf="SP", identificador_linha=2),
        LinhaImportacao(nome="Acme", cidade=" ", uf="SP", identificador_linha=3),
        LinhaImportacao(nome="Beta", cidade="Rio", uf="RJS", identificador_linha=4),
    ]
    resultado = await caso.executar(linhas)

    assert resultado.total_linhas == 3
    assert resultado.importados == 0
    assert resultado.duplicados == 0
    motivos = {e.linha: e.motivo for e in resultado.erros}
    assert "nome" in motivos[2]
    assert "cidade" in motivos[3]
    assert "uf" in motivos[4]


@pytest.mark.asyncio
async def test_duplicatas_no_lote_sao_ignoradas() -> None:
    repo = _RepoFake()
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    linhas = [
        LinhaImportacao(nome="Acme", cidade="SP", uf="SP", identificador_linha=2),
        LinhaImportacao(nome="Acme", cidade="SP", uf="SP", identificador_linha=3),
    ]
    resultado = await caso.executar(linhas)

    assert resultado.importados == 1
    assert resultado.duplicados == 1
    assert len(repo.persistidos) == 1


@pytest.mark.asyncio
async def test_duplicata_existente_no_banco_e_ignorada() -> None:
    repo = _RepoFake(existentes=[("Acme", "SP", "SP")])
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    linhas = [
        LinhaImportacao(nome="Acme", cidade="SP", uf="SP", identificador_linha=2),
        LinhaImportacao(nome="Beta", cidade="RJ", uf="RJ", identificador_linha=3),
    ]
    resultado = await caso.executar(linhas)

    assert resultado.importados == 1
    assert resultado.duplicados == 1
    assert [f.nome for f in repo.persistidos] == ["Beta"]


@pytest.mark.asyncio
async def test_lote_vazio_noop() -> None:
    repo = _RepoFake()
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar([])

    assert resultado.total_linhas == 0
    assert resultado.importados == 0
    assert resultado.duplicados == 0
    assert resultado.erros == []
    assert repo.persistidos == []
    assert repo.lotes == []


@pytest.mark.asyncio
async def test_importacao_nao_faz_geocodificacao() -> None:
    repo = _RepoFake()
    caso = ImportarFornecedores(repositorio=repo)  # type: ignore[arg-type]

    linhas = [LinhaImportacao(nome="Acme", cidade="São Paulo", uf="SP", identificador_linha=2)]
    await caso.executar(linhas)

    f = repo.persistidos[0]
    assert f.municipio_id is None
    assert f.lat is None
    assert f.lon is None
