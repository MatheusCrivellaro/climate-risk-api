"""Testes unitários de :class:`ReprocessarJob` com repositório fake."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pytest

from climate_risk.application.jobs.reprocessar import ReprocessarJob
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.excecoes import ErroJobEstadoInvalido, ErroJobNaoEncontrado


@dataclass
class _RepoFake:
    jobs: list[Job] = field(default_factory=list)
    salvos: list[Job] = field(default_factory=list)

    async def buscar_por_id(self, job_id: str) -> Job | None:
        for j in self.jobs:
            if j.id == job_id:
                return j
        return None

    async def salvar(self, job: Job) -> None:
        self.salvos.append(job)
        self.jobs = [job if j.id == job.id else j for j in self.jobs]

    async def listar(
        self,
        status: str | None = None,
        tipo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        return list(self.jobs)

    async def contar(self, status: str | None = None, tipo: str | None = None) -> int:
        return len(self.jobs)


def _job(status: str, *, tentativas: int = 3, erro: str | None = "boom") -> Job:
    return Job(
        id="job_x",
        tipo="noop",
        payload={},
        status=status,
        tentativas=tentativas,
        max_tentativas=3,
        criado_em=datetime.fromisoformat("2026-04-16T10:00:00+00:00"),
        iniciado_em=datetime.fromisoformat("2026-04-16T10:01:00+00:00"),
        concluido_em=datetime.fromisoformat("2026-04-16T10:02:00+00:00"),
        heartbeat=datetime.fromisoformat("2026-04-16T10:01:30+00:00"),
        erro=erro,
        proxima_tentativa_em=None,
    )


@pytest.mark.asyncio
async def test_reprocessar_job_failed_reseta_e_salva() -> None:
    repo = _RepoFake(jobs=[_job(StatusJob.FAILED)])
    caso = ReprocessarJob(repositorio=repo)  # type: ignore[arg-type]

    reenfileirado = await caso.executar("job_x")

    assert reenfileirado.status == StatusJob.PENDING
    assert reenfileirado.tentativas == 0
    assert reenfileirado.erro is None
    assert reenfileirado.iniciado_em is None
    assert reenfileirado.concluido_em is None
    assert reenfileirado.heartbeat is None
    assert reenfileirado.proxima_tentativa_em is None
    assert len(repo.salvos) == 1
    assert repo.salvos[0].id == "job_x"


@pytest.mark.asyncio
async def test_reprocessar_inexistente_levanta_nao_encontrado() -> None:
    repo = _RepoFake(jobs=[])
    caso = ReprocessarJob(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroJobNaoEncontrado):
        await caso.executar("job_fantasma")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [StatusJob.PENDING, StatusJob.RUNNING, StatusJob.COMPLETED, StatusJob.CANCELED],
)
async def test_reprocessar_estado_invalido_levanta(status: str) -> None:
    repo = _RepoFake(jobs=[_job(status)])
    caso = ReprocessarJob(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroJobEstadoInvalido) as exc:
        await caso.executar("job_x")
    assert exc.value.estado_atual == status
    assert exc.value.transicao == "retry"
    assert repo.salvos == []
