"""Testes unitários de :class:`ConsultarStats`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.resultados import ConsultarStats
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
    GrupoAgregadoRaw,
)


@dataclass
class _RepoFake:
    cenarios: list[str] = field(default_factory=list)
    anos: list[int] = field(default_factory=list)
    variaveis: list[str] = field(default_factory=list)
    nomes_indices: list[str] = field(default_factory=list)
    total_exec: int = 0
    total_res: int = 0

    async def salvar_lote(self, _: Sequence[ResultadoIndice]) -> None:
        return None

    async def listar(self, **_: object) -> list[ResultadoIndice]:
        return []

    async def contar(self, **_: object) -> int:
        return 0

    async def municipios_com_resultados(self, _: set[int]) -> set[int]:
        return set()

    async def consultar(
        self,
        filtros: FiltrosConsultaResultados,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        return []

    async def contar_por_filtros(self, _: FiltrosConsultaResultados) -> int:
        return 0

    async def agregar(
        self, _: FiltrosAgregacaoResultados
    ) -> list[GrupoAgregadoRaw]:
        return []

    async def distinct_cenarios(self) -> list[str]:
        return list(self.cenarios)

    async def distinct_anos(self) -> list[int]:
        return list(self.anos)

    async def distinct_variaveis(self) -> list[str]:
        return list(self.variaveis)

    async def distinct_nomes_indices(self) -> list[str]:
        return list(self.nomes_indices)

    async def contar_execucoes_com_resultados(self) -> int:
        return self.total_exec

    async def contar_resultados(self) -> int:
        return self.total_res


@pytest.mark.asyncio
async def test_propaga_dimensoes_e_counters() -> None:
    repo = _RepoFake(
        cenarios=["rcp45", "rcp85"],
        anos=[2026, 2027],
        variaveis=["pr"],
        nomes_indices=["PRCPTOT", "CDD"],
        total_exec=3,
        total_res=42,
    )
    caso = ConsultarStats(repositorio=repo)  # type: ignore[arg-type]

    stats = await caso.executar()

    assert stats.cenarios == ["rcp45", "rcp85"]
    assert stats.anos == [2026, 2027]
    assert stats.variaveis == ["pr"]
    assert stats.nomes_indices == ["PRCPTOT", "CDD"]
    assert stats.total_execucoes_com_resultados == 3
    assert stats.total_resultados == 42


@pytest.mark.asyncio
async def test_banco_vazio_retorna_tudo_zerado() -> None:
    caso = ConsultarStats(repositorio=_RepoFake())  # type: ignore[arg-type]
    stats = await caso.executar()
    assert stats.cenarios == []
    assert stats.total_execucoes_com_resultados == 0
    assert stats.total_resultados == 0
