"""Testes unitários de :class:`CancelarExecucao`."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest

from climate_risk.application.execucoes.cancelar import CancelarExecucao
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.job import Job
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada, ErroJobEstadoInvalido


@dataclass
class _RepoFake:
    itens: dict[str, Execucao]

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        return self.itens.get(execucao_id)

    async def salvar(self, execucao: Execucao) -> None:
        self.itens[execucao.id] = execucao

    async def listar(self, **_: Any) -> list[Execucao]:
        return list(self.itens.values())

    async def contar(self, **_: Any) -> int:
        return len(self.itens)


@dataclass
class _FilaFake:
    cancelados: list[str] = field(default_factory=list)

    async def enfileirar(self, tipo: str, payload: dict[str, Any], max_tentativas: int = 3) -> Job:
        raise AssertionError("enfileirar não deveria ser chamado no cancelamento")

    async def adquirir_proximo(self) -> Job | None:
        return None

    async def atualizar_heartbeat(self, job_id: str) -> None:
        pass

    async def concluir_com_sucesso(self, job_id: str) -> None:
        pass

    async def concluir_com_falha(
        self, job_id: str, erro: str, proxima_tentativa_em: datetime | None
    ) -> None:
        pass

    async def cancelar(self, job_id: str) -> bool:
        self.cancelados.append(job_id)
        return True

    async def recuperar_zumbis(self, timeout_segundos: int) -> int:
        return 0


def _execucao_pending(id_: str, job_id: str | None = "job_1") -> Execucao:
    return Execucao(
        id=id_,
        cenario="rcp45",
        variavel="pr",
        arquivo_origem="/tmp/x.nc",
        tipo="grade_bbox",
        parametros={},
        status=StatusExecucao.PENDING,
        criado_em=utc_now(),
        concluido_em=None,
        job_id=job_id,
    )


@pytest.mark.asyncio
async def test_cancela_execucao_pending_e_job() -> None:
    repo = _RepoFake(itens={"exec_1": _execucao_pending("exec_1", "job_abc")})
    fila = _FilaFake()
    caso = CancelarExecucao(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    atualizada = await caso.executar("exec_1")

    assert atualizada.status == StatusExecucao.CANCELED
    assert atualizada.concluido_em is not None
    assert repo.itens["exec_1"].status == StatusExecucao.CANCELED
    assert fila.cancelados == ["job_abc"]


@pytest.mark.asyncio
async def test_cancelar_sem_job_id_nao_chama_fila() -> None:
    repo = _RepoFake(itens={"exec_1": _execucao_pending("exec_1", job_id=None)})
    fila = _FilaFake()
    caso = CancelarExecucao(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    await caso.executar("exec_1")

    assert fila.cancelados == []


@pytest.mark.asyncio
async def test_cancelar_inexistente_levanta() -> None:
    repo = _RepoFake(itens={})
    fila = _FilaFake()
    caso = CancelarExecucao(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]
    with pytest.raises(ErroEntidadeNaoEncontrada):
        await caso.executar("exec_x")


@pytest.mark.asyncio
async def test_cancelar_em_estado_nao_pending_levanta() -> None:
    execucao = _execucao_pending("exec_1")
    terminal = Execucao(
        id=execucao.id,
        cenario=execucao.cenario,
        variavel=execucao.variavel,
        arquivo_origem=execucao.arquivo_origem,
        tipo=execucao.tipo,
        parametros=execucao.parametros,
        status=StatusExecucao.RUNNING,
        criado_em=execucao.criado_em,
        concluido_em=None,
        job_id=execucao.job_id,
    )
    repo = _RepoFake(itens={"exec_1": terminal})
    fila = _FilaFake()
    caso = CancelarExecucao(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    with pytest.raises(ErroJobEstadoInvalido):
        await caso.executar("exec_1")
    assert fila.cancelados == []
