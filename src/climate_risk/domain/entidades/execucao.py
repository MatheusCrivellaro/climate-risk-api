"""Entidade Execução.

Representa uma execução de cálculo de índices climáticos (UC-02 para grade ou
UC-03 para pontos). Uma Execução é **o que** foi pedido; o :class:`Job`
associado é **como/quando** o trabalho acontece.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class StatusExecucao:
    """Valores válidos para ``Execucao.status``.

    Mantemos como atributos de classe (em vez de ``Enum``) porque a coluna é
    persistida como ``TEXT`` e restringida por ``CHECK constraint`` em SQL —
    evita conversão de tipo ao ler do banco.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    TODOS: tuple[str, ...] = (PENDING, RUNNING, COMPLETED, FAILED, CANCELED)


@dataclass(frozen=True)
class Execucao:
    """Execução de cálculo de índices.

    Atributos:
        id: ULID com prefixo ``"exec_"``.
        cenario: Rótulo do cenário CORDEX (ex.: ``"rcp45"``, ``"historical"``).
        variavel: Nome da variável NetCDF (MVP: apenas ``"pr"``).
        arquivo_origem: Caminho do ``.nc`` usado como entrada.
        tipo: ``"grade_bbox"`` para UC-02 ou ``"pontos"`` para UC-03.
        parametros: Parâmetros livres da execução. Serializado como JSON em
            ``TEXT`` na camada de infra.
        status: Um dos valores de :class:`StatusExecucao`.
        criado_em: UTC, sempre preenchido.
        concluido_em: UTC, preenchido quando status vira terminal.
        job_id: Referência ao :class:`Job` que processou a execução; ``None``
            enquanto síncrono (UC-03 abaixo do limite) ou antes de enfileirar.
    """

    id: str
    cenario: str
    variavel: str
    arquivo_origem: str
    tipo: str
    parametros: dict[str, Any]
    status: str
    criado_em: datetime
    concluido_em: datetime | None
    job_id: str | None
