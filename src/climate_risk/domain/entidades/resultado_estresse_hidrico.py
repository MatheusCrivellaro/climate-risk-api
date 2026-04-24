"""Entidade :class:`ResultadoEstresseHidrico`.

Diferente do :class:`ResultadoIndice` (formato *long*, uma linha por índice),
este formato **wide** agrupa frequência (``dias secos quentes``) e intensidade
(``mm``) na mesma linha, indexadas por ``(execucao_id, municipio_id, ano,
cenario)``. Frequência e intensidade "andam juntas" — sempre consumidas em
par — e o formato wide torna consultas tipo "média ponderada" triviais.

Para adicionar novos indicadores, alterar o schema da tabela
``resultado_estresse_hidrico`` via Alembic migration (não reaproveita
``ResultadoIndice``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ResultadoEstresseHidrico:
    """Índices anuais de estresse hídrico por município/cenário (formato wide).

    Atributos:
        id: ULID com prefixo ``"reh_"``.
        execucao_id: FK para :class:`Execucao`.
        municipio_id: Código IBGE do município (int).
        ano: Ano do resultado (ex.: ``2030``).
        cenario: Rótulo CORDEX (``"rcp45"``, ``"rcp85"``, ``"ssp245"`` etc).
        frequencia_dias_secos_quentes: Contagem de dias secos quentes no ano.
        intensidade_mm: Soma do déficit hídrico (``evap - pr``) nos dias
            secos quentes, em mm.
        criado_em: Timestamp UTC de persistência.
    """

    id: str
    execucao_id: str
    municipio_id: int
    ano: int
    cenario: str
    frequencia_dias_secos_quentes: int
    intensidade_mm: float
    criado_em: datetime
