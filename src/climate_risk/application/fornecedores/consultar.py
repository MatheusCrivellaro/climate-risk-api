"""Caso de uso :class:`ConsultarFornecedores` — listagem paginada + busca por id."""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.portas.repositorios import RepositorioFornecedores

__all__ = [
    "ConsultarFornecedores",
    "FiltrosConsultaFornecedores",
    "PaginaFornecedores",
]


@dataclass(frozen=True)
class FiltrosConsultaFornecedores:
    """Filtros e paginação."""

    uf: str | None = None
    cidade: str | None = None
    limit: int = 100
    offset: int = 0


@dataclass(frozen=True)
class PaginaFornecedores:
    """Página paginada retornada pela listagem."""

    total: int
    limit: int
    offset: int
    itens: list[Fornecedor]


class ConsultarFornecedores:
    """Wrapper CRUD sobre :class:`RepositorioFornecedores`."""

    def __init__(self, repositorio: RepositorioFornecedores) -> None:
        self._repo = repositorio

    async def listar(self, filtros: FiltrosConsultaFornecedores) -> PaginaFornecedores:
        itens = await self._repo.listar(
            uf=filtros.uf,
            cidade=filtros.cidade,
            limit=filtros.limit,
            offset=filtros.offset,
        )
        total = await self._repo.contar(uf=filtros.uf, cidade=filtros.cidade)
        return PaginaFornecedores(
            total=total,
            limit=filtros.limit,
            offset=filtros.offset,
            itens=itens,
        )

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor:
        """Retorna o fornecedor ou levanta :class:`ErroEntidadeNaoEncontrada`."""
        fornecedor = await self._repo.buscar_por_id(fornecedor_id)
        if fornecedor is None:
            raise ErroEntidadeNaoEncontrada(entidade="Fornecedor", identificador=fornecedor_id)
        return fornecedor
