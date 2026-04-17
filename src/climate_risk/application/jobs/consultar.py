"""Caso de uso :class:`ConsultarJobs` — listagem e busca por id.

Simples wrapper sobre :class:`RepositorioJobs`. Existe como caso de uso
(e não como chamada direta ao repositório a partir da rota) para manter a
regra de que :mod:`interfaces` só conversa com :mod:`application`.
"""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.domain.entidades.job import Job
from climate_risk.domain.excecoes import ErroJobNaoEncontrado
from climate_risk.domain.portas.repositorios import RepositorioJobs


@dataclass(frozen=True)
class ResultadoListaJobs:
    """Agregado retornado por :meth:`ConsultarJobs.listar`."""

    total: int
    limit: int
    offset: int
    items: list[Job]


class ConsultarJobs:
    """Lê a tabela de jobs via :class:`RepositorioJobs` (CRUD)."""

    def __init__(self, repositorio: RepositorioJobs) -> None:
        self._repositorio = repositorio

    async def buscar_por_id(self, job_id: str) -> Job:
        """Retorna o job ou levanta :class:`ErroJobNaoEncontrado`."""
        job = await self._repositorio.buscar_por_id(job_id)
        if job is None:
            raise ErroJobNaoEncontrado(job_id)
        return job

    async def listar(
        self,
        *,
        status: str | None = None,
        tipo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> ResultadoListaJobs:
        items = await self._repositorio.listar(status=status, tipo=tipo, limit=limit, offset=offset)
        total = await self._repositorio.contar(status=status, tipo=tipo)
        return ResultadoListaJobs(total=total, limit=limit, offset=offset, items=items)
