"""Caso de uso :class:`ConsultarExecucoes` — listagem e busca por id."""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.domain.entidades.execucao import Execucao
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = ["ConsultarExecucoes", "ResultadoListaExecucoes"]


@dataclass(frozen=True)
class ResultadoListaExecucoes:
    """Agregado retornado por :meth:`ConsultarExecucoes.listar`."""

    total: int
    limit: int
    offset: int
    items: list[Execucao]


class ConsultarExecucoes:
    """Wrapper CRUD sobre :class:`RepositorioExecucoes`.

    Mantém a regra de ouro: camada ``interfaces`` fala com ``application``,
    nunca diretamente com o repositório.
    """

    def __init__(self, repositorio: RepositorioExecucoes) -> None:
        self._repositorio = repositorio

    async def buscar_por_id(self, execucao_id: str) -> Execucao:
        """Retorna a execução ou levanta :class:`ErroEntidadeNaoEncontrada`."""
        execucao = await self._repositorio.buscar_por_id(execucao_id)
        if execucao is None:
            raise ErroEntidadeNaoEncontrada(entidade="Execucao", identificador=execucao_id)
        return execucao

    async def listar(
        self,
        *,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> ResultadoListaExecucoes:
        items = await self._repositorio.listar(
            cenario=cenario, variavel=variavel, status=status, limit=limit, offset=offset
        )
        total = await self._repositorio.contar(cenario=cenario, variavel=variavel, status=status)
        return ResultadoListaExecucoes(total=total, limit=limit, offset=offset, items=items)
