"""Testes unitários de :class:`CriarExecucaoPorPontos` (Slice 7 — lado síncrono)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from climate_risk.application.calculos.calcular_por_pontos import PontoEntradaDominio
from climate_risk.application.calculos.criar_execucao_por_pontos import (
    CriarExecucaoPorPontos,
    ParametrosCriacaoExecucaoPontos,
)
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.job import Job, StatusJob
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

    async def listar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Execucao]:
        return list(self.salvos)

    async def contar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
    ) -> int:
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


def _pontos(n: int) -> list[PontoEntradaDominio]:
    return [
        PontoEntradaDominio(lat=-22.9 + 0.01 * i, lon=-46.5, identificador=f"P{i}")
        for i in range(n)
    ]


def _params(arquivo_nc: str, pontos: list[PontoEntradaDominio]) -> ParametrosCriacaoExecucaoPontos:
    return ParametrosCriacaoExecucaoPontos(
        arquivo_nc=arquivo_nc,
        cenario="rcp45",
        variavel="pr",
        pontos=pontos,
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
    caso = CriarExecucaoPorPontos(repositorio_execucoes=repo, fila_jobs=fila)

    pontos = _pontos(150)
    resultado = await caso.executar(_params(str(arquivo), pontos))

    # Persistiu 2x: criação inicial + upsert com job_id.
    assert len(repo.salvos) == 2
    primeira = repo.salvos[0]
    assert primeira.status == StatusExecucao.PENDING
    assert primeira.tipo == "pontos_lote"
    assert primeira.job_id is None
    assert primeira.concluido_em is None

    segunda = repo.salvos[1]
    assert segunda.id == primeira.id
    assert segunda.job_id == resultado.job_id
    assert segunda.status == StatusExecucao.PENDING

    # Fila recebeu 1 job do tipo calcular_pontos com payload correto.
    assert len(fila.enfileirados) == 1
    job = fila.enfileirados[0]
    assert job.tipo == "calcular_pontos"
    assert job.payload["execucao_id"] == primeira.id
    assert job.payload["arquivo_nc"] == str(arquivo)
    assert job.payload["cenario"] == "rcp45"
    assert job.payload["variavel"] == "pr"
    assert job.payload["p95_baseline"] == {"inicio": 2026, "fim": 2027}
    assert job.payload["parametros_indices"]["heavy_thresholds"] == [20.0, 50.0]

    # Pontos serializados como lista de dicts preservando ordem e identificador.
    serializados = job.payload["pontos"]
    assert len(serializados) == 150
    assert serializados[0] == {"lat": -22.9, "lon": -46.5, "identificador": "P0"}
    assert serializados[149]["identificador"] == "P149"

    assert resultado.total_pontos == 150
    assert resultado.status == StatusExecucao.PENDING


@pytest.mark.asyncio
async def test_sem_baseline_envia_none(tmp_path: Path) -> None:
    arquivo = tmp_path / "pr.nc"
    arquivo.write_bytes(b"")
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CriarExecucaoPorPontos(repositorio_execucoes=repo, fila_jobs=fila)

    params = ParametrosCriacaoExecucaoPontos(
        arquivo_nc=str(arquivo),
        cenario="rcp45",
        variavel="pr",
        pontos=_pontos(101),
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=None,
        p95_wet_thr=1.0,
    )
    await caso.executar(params)

    assert fila.enfileirados[0].payload["p95_baseline"] is None


@pytest.mark.asyncio
async def test_arquivo_ausente_falha_antes_de_persistir(tmp_path: Path) -> None:
    inexistente = tmp_path / "no.nc"
    repo = _RepoExecucoesFake(salvos=[])
    fila = _FilaJobsFake(enfileirados=[])
    caso = CriarExecucaoPorPontos(repositorio_execucoes=repo, fila_jobs=fila)

    with pytest.raises(ErroArquivoNCNaoEncontrado):
        await caso.executar(_params(str(inexistente), _pontos(150)))

    assert repo.salvos == []
    assert fila.enfileirados == []
