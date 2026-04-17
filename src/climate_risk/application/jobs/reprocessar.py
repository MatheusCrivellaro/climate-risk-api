"""Caso de uso :class:`ReprocessarJob` — retry manual de jobs ``failed``.

Regra de negócio: só jobs em estado ``failed`` podem ser reprocessados.
A operação zera ``tentativas``, limpa ``erro``/``concluido_em``/``proxima_tentativa_em``
e volta o status para ``pending``. O Worker pega naturalmente no próximo
ciclo de polling.

Estados válidos para esta transição: ``failed``. Qualquer outro estado
levanta :class:`ErroJobEstadoInvalido` (traduzido para ``409`` pelo
middleware HTTP).
"""

from __future__ import annotations

from dataclasses import replace

from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.excecoes import ErroJobEstadoInvalido, ErroJobNaoEncontrado
from climate_risk.domain.portas.repositorios import RepositorioJobs


class ReprocessarJob:
    """Reenfileira um job ``failed`` resetando tentativas e erro."""

    def __init__(self, repositorio: RepositorioJobs) -> None:
        self._repositorio = repositorio

    async def executar(self, job_id: str) -> Job:
        job = await self._repositorio.buscar_por_id(job_id)
        if job is None:
            raise ErroJobNaoEncontrado(job_id)
        if job.status != StatusJob.FAILED:
            raise ErroJobEstadoInvalido(job_id=job_id, estado_atual=job.status, transicao="retry")

        reenfileirado = replace(
            job,
            status=StatusJob.PENDING,
            tentativas=0,
            erro=None,
            iniciado_em=None,
            concluido_em=None,
            heartbeat=None,
            proxima_tentativa_em=None,
        )
        await self._repositorio.salvar(reenfileirado)
        return reenfileirado
