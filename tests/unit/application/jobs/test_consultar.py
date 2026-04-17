"""Testes unitários de :class:`ConsultarJobs` com repositório fake."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pytest

from climate_risk.application.jobs.consultar import ConsultarJobs
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.excecoes import ErroJobNaoEncontrado


@dataclass
class _RepoFake:
    jobs: list[Job] = field(default_factory=list)

    async def buscar_por_id(self, job_id: str) -> Job | None:
        for j in self.jobs:
            if j.id == job_id:
                return j
        return None

    async def salvar(self, job: Job) -> None:
        self.jobs = [job if j.id == job.id else j for j in self.jobs]
        if not any(j.id == job.id for j in self.jobs):
            self.jobs.append(job)

    async def listar(
        self,
        status: str | None = None,
        tipo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        out = [
            j
            for j in self.jobs
            if (status is None or j.status == status) and (tipo is None or j.tipo == tipo)
        ]
        return out[offset : offset + limit]

    async def contar(self, status: str | None = None, tipo: str | None = None) -> int:
        return len(
            [
                j
                for j in self.jobs
                if (status is None or j.status == status) and (tipo is None or j.tipo == tipo)
            ]
        )


def _job(job_id: str, status: str = StatusJob.PENDING, tipo: str = "noop") -> Job:
    return Job(
        id=job_id,
        tipo=tipo,
        payload={"n": 1},
        status=status,
        tentativas=0,
        max_tentativas=3,
        criado_em=datetime.fromisoformat("2026-04-16T10:00:00+00:00"),
        iniciado_em=None,
        concluido_em=None,
        heartbeat=None,
        erro=None,
        proxima_tentativa_em=None,
    )


@pytest.mark.asyncio
async def test_buscar_por_id_retorna_job() -> None:
    repo = _RepoFake(jobs=[_job("job_1")])
    caso = ConsultarJobs(repositorio=repo)  # type: ignore[arg-type]

    job = await caso.buscar_por_id("job_1")
    assert job.id == "job_1"


@pytest.mark.asyncio
async def test_buscar_por_id_inexistente_levanta() -> None:
    repo = _RepoFake(jobs=[])
    caso = ConsultarJobs(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroJobNaoEncontrado) as exc:
        await caso.buscar_por_id("job_fantasma")
    assert exc.value.job_id == "job_fantasma"


@pytest.mark.asyncio
async def test_listar_aplica_filtros_e_conta() -> None:
    repo = _RepoFake(
        jobs=[
            _job("job_1", status=StatusJob.PENDING),
            _job("job_2", status=StatusJob.COMPLETED),
            _job("job_3", status=StatusJob.PENDING),
        ]
    )
    caso = ConsultarJobs(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.listar(status=StatusJob.PENDING, limit=10, offset=0)
    assert resultado.total == 2
    assert resultado.limit == 10
    assert resultado.offset == 0
    assert {j.id for j in resultado.items} == {"job_1", "job_3"}
