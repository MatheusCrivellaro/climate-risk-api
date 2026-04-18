"""Caso de uso :class:`CancelarExecucao` — transição ``pending → canceled``.

Regras de negócio:

- Só execuções em ``pending`` podem ser canceladas pelo usuário. Uma
  execução ``running`` está sendo processada por um worker; cancelar
  precisaria de sinalização adicional que o MVP não implementa. Estados
  terminais (``completed``/``failed``/``canceled``) também não
  permitem transição (idempotência explícita via erro).
- O :class:`Job` associado, se ainda estiver ``pending`` na fila, é
  cancelado em conjunto via :class:`FilaJobs.cancelar`. Se o job já
  saiu de ``pending`` (ex.: worker pegou entre a checagem e o update),
  a cancelação do job é no-op (retorna ``False``) — a execução ainda é
  marcada como ``canceled`` porque o cancelamento é semântica do
  usuário sobre a execução, não sobre o worker.
"""

from __future__ import annotations

from dataclasses import replace

from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada, ErroJobEstadoInvalido
from climate_risk.domain.portas.fila_jobs import FilaJobs
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = ["CancelarExecucao"]


class CancelarExecucao:
    """Cancela uma :class:`Execucao` ainda em ``pending``."""

    def __init__(
        self,
        repositorio_execucoes: RepositorioExecucoes,
        fila_jobs: FilaJobs,
    ) -> None:
        self._repo = repositorio_execucoes
        self._fila = fila_jobs

    async def executar(self, execucao_id: str) -> Execucao:
        execucao = await self._repo.buscar_por_id(execucao_id)
        if execucao is None:
            raise ErroEntidadeNaoEncontrada(entidade="Execucao", identificador=execucao_id)
        if execucao.status != StatusExecucao.PENDING:
            raise ErroJobEstadoInvalido(
                job_id=execucao_id,
                estado_atual=execucao.status,
                transicao="cancelar",
            )

        if execucao.job_id is not None:
            await self._fila.cancelar(execucao.job_id)

        cancelada = replace(
            execucao,
            status=StatusExecucao.CANCELED,
            concluido_em=utc_now(),
        )
        await self._repo.salvar(cancelada)
        return cancelada
