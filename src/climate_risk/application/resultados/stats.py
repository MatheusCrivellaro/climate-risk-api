"""Caso de uso :class:`ConsultarStats` (Slice 11).

Lista as dimensões disponíveis (cenários, anos, variáveis, nomes de
índice) e dois counters úteis para health checks e dashboards — tudo
delegado ao repositório.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from climate_risk.domain.portas.repositorios import RepositorioResultados

__all__ = ["ConsultarStats", "EstatisticasResultados"]


@dataclass(frozen=True)
class EstatisticasResultados:
    """Agregado devolvido por :meth:`ConsultarStats.executar`."""

    cenarios: list[str] = field(default_factory=list)
    anos: list[int] = field(default_factory=list)
    variaveis: list[str] = field(default_factory=list)
    nomes_indices: list[str] = field(default_factory=list)
    total_execucoes_com_resultados: int = 0
    total_resultados: int = 0


class ConsultarStats:
    """Orquestra as consultas ``distinct_*`` + counters do repositório."""

    def __init__(self, repositorio: RepositorioResultados) -> None:
        self._repositorio = repositorio

    async def executar(self) -> EstatisticasResultados:
        cenarios = await self._repositorio.distinct_cenarios()
        anos = await self._repositorio.distinct_anos()
        variaveis = await self._repositorio.distinct_variaveis()
        nomes_indices = await self._repositorio.distinct_nomes_indices()
        total_execucoes = await self._repositorio.contar_execucoes_com_resultados()
        total_resultados = await self._repositorio.contar_resultados()
        return EstatisticasResultados(
            cenarios=cenarios,
            anos=anos,
            variaveis=variaveis,
            nomes_indices=nomes_indices,
            total_execucoes_com_resultados=total_execucoes,
            total_resultados=total_resultados,
        )
