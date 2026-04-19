"""Caso de uso :class:`RemoverFornecedor` — delete com 404 explicita."""

from __future__ import annotations

from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.portas.repositorios import RepositorioFornecedores

__all__ = ["RemoverFornecedor"]


class RemoverFornecedor:
    """Remove o fornecedor; levanta 404 se não existir."""

    def __init__(self, repositorio: RepositorioFornecedores) -> None:
        self._repo = repositorio

    async def executar(self, fornecedor_id: str) -> None:
        removido = await self._repo.remover(fornecedor_id)
        if not removido:
            raise ErroEntidadeNaoEncontrada(entidade="Fornecedor", identificador=fornecedor_id)
