"""Porta :class:`FilaJobs` — contrato da fila de jobs assíncronos.

A camada :mod:`application` enfileira trabalhos; a camada
:mod:`infrastructure.fila` fornece uma implementação concreta (``FilaSQLite``
no Slice 5). O :mod:`infrastructure.fila.worker` consome via esta porta.

Responsabilidades cobertas pelo contrato:

- Enfileirar novos jobs (``enfileirar``).
- Permitir aquisição **atômica** por workers (``adquirir_proximo``).
- Marcar jobs como iniciados/concluídos/falhados.
- Atualizar heartbeat durante execução longa.
- Recuperar jobs zumbis (status ``running`` sem heartbeat recente).
- Cancelar jobs ainda ``pending``.

Responsabilidades **fora** deste contrato:

- Executar o código de cada ``tipo`` de job (responsabilidade do Worker).
- Serializar/desserializar o ``payload`` (recebe/devolve ``dict`` pronto).
- Aplicar a regra de ``max_tentativas`` — é o Worker quem decide se o
  próximo status após falha é ``pending`` (com backoff) ou ``failed``.

ADR-005: imports restritos a :mod:`stdlib` e :mod:`domain`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from climate_risk.domain.entidades.job import Job


class FilaJobs(Protocol):
    """Contrato para a fila de jobs assíncronos (ADR-004)."""

    async def enfileirar(
        self,
        tipo: str,
        payload: dict[str, Any],
        max_tentativas: int = 3,
    ) -> Job:
        """Cria e persiste um job novo com ``status=pending``.

        Args:
            tipo: Categoria do job (ex.: ``"noop"``, ``"processar_cordex"``).
            payload: Dicionário arbitrário; a implementação serializa em JSON.
            max_tentativas: Limite antes de marcar ``failed``. Default 3
                (ADR-004).

        Returns:
            :class:`Job` recém-criado com ``id`` já preenchido.
        """
        ...

    async def adquirir_proximo(self) -> Job | None:
        """Tenta adquirir atomicamente o próximo job elegível.

        Elegibilidade:

        - ``status = 'pending'``;
        - ``proxima_tentativa_em`` é ``None`` **ou** menor/igual a agora
          (permite backoff após falha).

        **Garantia de atomicidade:** dois chamadores concorrentes nunca
        recebem o mesmo job. Em SQLite é alcançado via ``UPDATE ... WHERE
        status='pending' RETURNING`` com sub-SELECT no próprio ``WHERE``
        (instrução única, lock de escrita serializa).

        Returns:
            :class:`Job` com ``status='running'``, ``iniciado_em=now``,
            ``heartbeat=now``; ou ``None`` se não há job elegível.
        """
        ...

    async def atualizar_heartbeat(self, job_id: str) -> None:
        """Atualiza apenas o campo ``heartbeat`` do job para ``utc_now()``.

        Chamado periodicamente pelo worker durante execuções longas para
        sinalizar que o processo está vivo. Se o job não existe ou já está
        em estado terminal, a operação é **no-op** (evita race ao final da
        execução).
        """
        ...

    async def concluir_com_sucesso(self, job_id: str) -> None:
        """Transição ``running → completed``.

        Atualiza ``status='completed'``, ``concluido_em=now`` e limpa
        ``proxima_tentativa_em``.
        """
        ...

    async def concluir_com_falha(
        self,
        job_id: str,
        erro: str,
        proxima_tentativa_em: datetime | None,
    ) -> None:
        """Registra falha e decide destino conforme parâmetros do chamador.

        Política desta porta (propositalmente dumb):

        - ``tentativas += 1`` sempre.
        - ``erro = <msg>``.
        - Se ``proxima_tentativa_em is None``: status final ``failed``,
          ``concluido_em=now``.
        - Caso contrário: status volta a ``pending`` com
          ``proxima_tentativa_em`` preenchido (agendamento de retry).

        O Worker calcula ``proxima_tentativa_em`` com base em
        ``max_tentativas``; a fila apenas executa o que foi pedido.
        """
        ...

    async def cancelar(self, job_id: str) -> bool:
        """Transição ``pending → canceled``.

        Returns:
            ``True`` se o job estava em ``pending`` e foi cancelado;
            ``False`` caso contrário (inclui inexistente e qualquer outro
            status).
        """
        ...

    async def recuperar_zumbis(self, timeout_segundos: int) -> int:
        """Devolve à fila jobs ``running`` com heartbeat antigo.

        Critério: ``status='running'`` e ``heartbeat < now - timeout``.
        Ação: ``status='pending'``, ``tentativas += 1``,
        ``proxima_tentativa_em = None`` (reprocessa imediatamente).

        Returns:
            Quantidade de jobs recuperados no sweep.
        """
        ...
