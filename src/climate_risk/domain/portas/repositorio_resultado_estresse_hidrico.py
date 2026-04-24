"""Porta :class:`RepositorioResultadoEstresseHidrico`.

Contrato do repositório de :class:`ResultadoEstresseHidrico` (formato wide
— ver ADR-009 / Slice 15). Mantido separado de
:class:`RepositorioResultados` porque a tabela alvo é distinta e os filtros
aceitos divergem (sem ``nome_indice``, sem ``lat``/``lon``, sem BBox).

ADR-005: Protocol em domínio; implementação em ``infrastructure`` sob
``SQLAlchemyRepositorioResultadoEstresseHidrico``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)


class RepositorioResultadoEstresseHidrico(Protocol):
    """Persistência e consulta de :class:`ResultadoEstresseHidrico`."""

    async def salvar_lote(
        self,
        resultados: Iterable[ResultadoEstresseHidrico],
    ) -> None:
        """Persiste uma sequência de resultados em uma transação."""
        ...

    async def listar(
        self,
        *,
        execucao_id: str | None = None,
        cenario: str | None = None,
        ano: int | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        municipio_id: int | None = None,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoEstresseHidrico]:
        """Lista resultados aplicando filtros opcionais.

        Quando ``uf`` é fornecido, a implementação faz JOIN com
        ``municipio`` para restringir por UF.
        """
        ...

    async def contar(
        self,
        *,
        execucao_id: str | None = None,
        cenario: str | None = None,
        ano: int | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        municipio_id: int | None = None,
        uf: str | None = None,
    ) -> int:
        """``COUNT(*)`` sob as mesmas condições de :meth:`listar`."""
        ...
