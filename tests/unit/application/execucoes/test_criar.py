"""Testes unitários de :class:`CriarExecucaoCordex`.

Cobre o lado síncrono do UC-02:

- Persiste ``Execucao`` em ``pending`` antes de enfileirar.
- Enfileira ``Job`` do tipo ``"processar_cordex"`` com payload serializável.
- Atualiza a execução com o ``job_id`` (segundo upsert).
- Levanta :class:`ErroArquivoNCNaoEncontrado` antes de qualquer side effect
  quando o arquivo não existe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from climate_risk.application.execucoes.criar import (
    CriarExecucaoCordex,
    ParametrosCriacaoExecucao,
)
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.job import Job, StatusJob
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline


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

    async def atualizar_heartbeat(self, job_id: str) -> None:
        pass

    async def concluir_com_sucesso(self, job_id: str) -> None:
        pass

    async def concluir_com_falha(
        self, job_id: str, erro: str, proxima_tentativa_em: datetime | None
    ) -> None:
        pass

    async def cancelar(self, job_id: str) -> bool:
        return True

    async def recuperar_zumbis(self, timeout_segundos: int) -> int:
        return 0


def _params(tmp_arquivo: str, *, bbox: BoundingBox | None = None) -> ParametrosCriacaoExecucao:
    return ParametrosCriacaoExecucao(
        arquivo_nc=tmp_arquivo,
        cenario="rcp45",
        variavel="pr",
        bbox=bbox,
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2027),
        p95_wet_thr=1.0,
    )


@pytest.mark.asyncio
async def test_cria_execucao_pending_e_enfileira_job(tmp_path: Path) -> None:
    arquivo = tmp_path / "pr.nc"
    arquivo.write_bytes(b"")
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CriarExecucaoCordex(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    bbox = BoundingBox(-10.0, 0.0, -50.0, -40.0)
    resultado = await caso.executar(_params(str(arquivo), bbox=bbox))

    # Salvou 2x: criação inicial + upsert com job_id.
    assert len(repo.salvos) == 2
    primeira = repo.salvos[0]
    assert primeira.status == StatusExecucao.PENDING
    assert primeira.tipo == "grade_bbox"
    assert primeira.job_id is None
    assert primeira.concluido_em is None

    segunda = repo.salvos[1]
    assert segunda.id == primeira.id
    assert segunda.job_id == resultado.job_id
    assert segunda.status == StatusExecucao.PENDING

    # Fila recebeu 1 job com payload JSON-friendly.
    assert len(fila.enfileirados) == 1
    job = fila.enfileirados[0]
    assert job.tipo == "processar_cordex"
    assert job.payload["execucao_id"] == primeira.id
    assert job.payload["bbox"] == {
        "lat_min": -10.0,
        "lat_max": 0.0,
        "lon_min": -50.0,
        "lon_max": -40.0,
    }
    assert job.payload["p95_baseline"] == {"inicio": 2026, "fim": 2027}
    assert job.payload["parametros_indices"]["heavy_thresholds"] == [20.0, 50.0]

    assert resultado.execucao_id == primeira.id
    assert resultado.status == StatusExecucao.PENDING


@pytest.mark.asyncio
async def test_sem_bbox_e_sem_baseline_envia_none(tmp_path: Path) -> None:
    arquivo = tmp_path / "pr.nc"
    arquivo.write_bytes(b"")
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CriarExecucaoCordex(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    params = ParametrosCriacaoExecucao(
        arquivo_nc=str(arquivo),
        cenario="rcp45",
        variavel="pr",
        bbox=None,
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=None,
        p95_wet_thr=1.0,
    )
    await caso.executar(params)

    payload = fila.enfileirados[0].payload
    assert payload["bbox"] is None
    assert payload["p95_baseline"] is None


@pytest.mark.asyncio
async def test_arquivo_ausente_falha_antes_de_persistir(tmp_path: Path) -> None:
    inexistente = tmp_path / "no.nc"
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CriarExecucaoCordex(repositorio_execucoes=repo, fila_jobs=fila)  # type: ignore[arg-type]

    with pytest.raises(ErroArquivoNCNaoEncontrado):
        await caso.executar(_params(str(inexistente)))

    assert repo.salvos == []
    assert fila.enfileirados == []
