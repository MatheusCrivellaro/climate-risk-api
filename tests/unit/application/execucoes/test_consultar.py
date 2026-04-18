"""Testes unitários de :class:`ConsultarExecucoes`."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from climate_risk.application.execucoes.consultar import ConsultarExecucoes
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada


@dataclass
class _RepoFake:
    itens: list[Execucao]
    chamadas_listar: list[dict[str, Any]] = field(default_factory=list)
    chamadas_contar: list[dict[str, Any]] = field(default_factory=list)

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        for e in self.itens:
            if e.id == execucao_id:
                return e
        return None

    async def salvar(self, execucao: Execucao) -> None:
        pass

    async def listar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Execucao]:
        self.chamadas_listar.append(
            {
                "cenario": cenario,
                "variavel": variavel,
                "status": status,
                "limit": limit,
                "offset": offset,
            }
        )
        filtrados = [
            e
            for e in self.itens
            if (cenario is None or e.cenario == cenario)
            and (variavel is None or e.variavel == variavel)
            and (status is None or e.status == status)
        ]
        return filtrados[offset : offset + limit]

    async def contar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
    ) -> int:
        self.chamadas_contar.append({"cenario": cenario, "variavel": variavel, "status": status})
        filtrados = [
            e
            for e in self.itens
            if (cenario is None or e.cenario == cenario)
            and (variavel is None or e.variavel == variavel)
            and (status is None or e.status == status)
        ]
        return len(filtrados)


def _execucao(id_: str, status: str = StatusExecucao.PENDING, cenario: str = "rcp45") -> Execucao:
    return Execucao(
        id=id_,
        cenario=cenario,
        variavel="pr",
        arquivo_origem="/tmp/x.nc",
        tipo="grade_bbox",
        parametros={},
        status=status,
        criado_em=utc_now(),
        concluido_em=None,
        job_id=None,
    )


@pytest.mark.asyncio
async def test_buscar_por_id_encontra() -> None:
    repo = _RepoFake(itens=[_execucao("exec_1")])
    caso = ConsultarExecucoes(repositorio=repo)
    ex = await caso.buscar_por_id("exec_1")
    assert ex.id == "exec_1"


@pytest.mark.asyncio
async def test_buscar_por_id_ausente_levanta() -> None:
    repo = _RepoFake(itens=[])
    caso = ConsultarExecucoes(repositorio=repo)
    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.buscar_por_id("exec_zzz")


@pytest.mark.asyncio
async def test_listar_encaminha_filtros_e_agrega_total() -> None:
    repo = _RepoFake(
        itens=[
            _execucao("exec_a", cenario="rcp45"),
            _execucao("exec_b", cenario="rcp85"),
            _execucao("exec_c", cenario="rcp45"),
        ]
    )
    caso = ConsultarExecucoes(repositorio=repo)
    resultado = await caso.listar(cenario="rcp45", limit=10, offset=0)
    assert resultado.total == 2
    assert resultado.limit == 10
    assert resultado.offset == 0
    assert [e.id for e in resultado.items] == ["exec_a", "exec_c"]
    assert repo.chamadas_listar[0]["cenario"] == "rcp45"
    assert repo.chamadas_contar[0]["cenario"] == "rcp45"
