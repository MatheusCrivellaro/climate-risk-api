"""Entidade Job.

Unidade de trabalho para a fila assíncrona (Slice 5). No Slice 2 apenas a
entidade e seu repositório CRUD existem — sem lógica de fila, heartbeat ou
retry (fica para Slice 5).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class StatusJob:
    """Valores válidos para ``Job.status``.

    Mantemos como atributos de classe (em vez de ``Enum``) pelo mesmo motivo
    de :class:`StatusExecucao`: coluna em ``TEXT`` com ``CHECK constraint`` em
    SQL, sem conversão ao ler.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    TODOS: tuple[str, ...] = (PENDING, RUNNING, COMPLETED, FAILED, CANCELED)


@dataclass(frozen=True)
class Job:
    """Trabalho assíncrono a ser executado pelo worker.

    Atributos:
        id: ULID com prefixo ``"job_"``.
        tipo: Categoria do trabalho (ex.: ``"processar_cordex"``,
            ``"calcular_pontos_lote"``).
        payload: Dados arbitrários necessários para executar o job.
            Serializado como JSON em ``TEXT`` na infra.
        status: Um dos valores de :class:`StatusJob`.
        tentativas: Quantidade de tentativas já feitas (inicia em 0).
        max_tentativas: Limite antes de marcar ``failed`` permanentemente.
        criado_em: UTC.
        iniciado_em: Primeira vez que o worker pegou o job.
        concluido_em: Última transição para status terminal.
        heartbeat: Última atualização do worker durante execução.
        erro: Mensagem da última falha, se houver.
        proxima_tentativa_em: Quando o agendador pode re-tentar (backoff).
    """

    id: str
    tipo: str
    payload: dict[str, Any]
    status: str
    tentativas: int
    max_tentativas: int
    criado_em: datetime
    iniciado_em: datetime | None
    concluido_em: datetime | None
    heartbeat: datetime | None
    erro: str | None
    proxima_tentativa_em: datetime | None
