"""Testes do método ``executar_de_pasta`` do caso de uso (Slice 17)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from climate_risk.application.indices.calcular_estresse_hidrico import (
    CalcularIndicesEstresseHidrico,
    ParametrosCalculoEstresseHidricoPasta,
)
from climate_risk.core.tempo import utc_now
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
)
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado


@dataclass
class _RepoExecucoesFake:
    salvos: list[Execucao]

    async def salvar(self, execucao: Execucao) -> None:
        self.salvos.append(execucao)

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None:
        for ex in reversed(self.salvos):
            if ex.id == execucao_id:
                return ex
        return None

    async def listar(self, **_: Any) -> list[Execucao]:
        return list(self.salvos)

    async def contar(self, **_: Any) -> int:
        return len(self.salvos)


@dataclass
class _FilaJobsFake:
    enfileirados: list[Job]

    async def enfileirar(self, tipo: str, payload: dict[str, Any], max_tentativas: int = 3) -> Job:
        job = Job(
            id=f"job_fake_{len(self.enfileirados):04d}",
            tipo=tipo,
            payload=payload,
            status=StatusJob.PENDING,
            tentativas=0,
            max_tentativas=max_tentativas,
            criado_em=utc_now(),
            iniciado_em=None,
            concluido_em=None,
            heartbeat=None,
            erro=None,
            proxima_tentativa_em=None,
        )
        self.enfileirados.append(job)
        return job

    async def adquirir_proximo(self) -> Job | None:
        return None

    async def atualizar_heartbeat(self, job_id: str) -> None: ...

    async def concluir_com_sucesso(self, job_id: str) -> None: ...

    async def concluir_com_falha(
        self, job_id: str, erro: str, proxima_tentativa_em: datetime | None
    ) -> None: ...

    async def cancelar(self, job_id: str) -> bool:
        return True

    async def recuperar_zumbis(self, timeout_segundos: int) -> int:
        return 0


def _params(
    pasta_pr: Path,
    pasta_tas: Path,
    pasta_evap: Path,
    *,
    cenario: str = "rcp45",
) -> ParametrosCalculoEstresseHidricoPasta:
    return ParametrosCalculoEstresseHidricoPasta(
        pasta_pr=pasta_pr,
        pasta_tas=pasta_tas,
        pasta_evap=pasta_evap,
        cenario=cenario,
        parametros_indices=ParametrosIndicesEstresseHidrico(
            limiar_pr_mm_dia=1.0, limiar_tas_c=30.0
        ),
    )


@pytest.mark.asyncio
async def test_executar_de_pasta_cria_execucao_e_enfileira_job_pasta(
    tmp_path: Path,
) -> None:
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CalcularIndicesEstresseHidrico(
        repositorio_execucoes=repo,  # type: ignore[arg-type]
        fila_jobs=fila,  # type: ignore[arg-type]
    )

    resultado = await caso.executar_de_pasta(_params(pasta_pr, pasta_tas, pasta_evap))

    assert len(repo.salvos) == 2
    primeira = repo.salvos[0]
    assert primeira.status == StatusExecucao.PENDING
    assert primeira.tipo == "estresse_hidrico"
    assert primeira.cenario == "rcp45"

    segunda = repo.salvos[1]
    assert segunda.id == primeira.id
    assert segunda.job_id == resultado.job_id

    assert len(fila.enfileirados) == 1
    job = fila.enfileirados[0]
    assert job.tipo == "processar_estresse_hidrico_pasta"
    assert job.payload["execucao_id"] == primeira.id
    assert job.payload["pasta_pr"] == str(pasta_pr)
    assert job.payload["pasta_tas"] == str(pasta_tas)
    assert job.payload["pasta_evap"] == str(pasta_evap)
    assert job.payload["cenario"] == "rcp45"
    assert job.payload["limiar_pr_mm_dia"] == 1.0
    assert job.payload["limiar_tas_c"] == 30.0


@pytest.mark.asyncio
async def test_executar_de_pasta_pasta_inexistente_falha(tmp_path: Path) -> None:
    pasta_pr = tmp_path / "pr-ausente"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    pasta_tas.mkdir()
    pasta_evap.mkdir()
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CalcularIndicesEstresseHidrico(
        repositorio_execucoes=repo,  # type: ignore[arg-type]
        fila_jobs=fila,  # type: ignore[arg-type]
    )

    with pytest.raises(ErroArquivoNCNaoEncontrado):
        await caso.executar_de_pasta(_params(pasta_pr, pasta_tas, pasta_evap))

    assert repo.salvos == []
    assert fila.enfileirados == []
