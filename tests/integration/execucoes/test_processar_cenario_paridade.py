"""Paridade do processamento CORDEX completo (UC-02) com o baseline legado.

Rodamos :class:`ProcessarCenarioCordex` contra a fixture sintética
``cordex_sintetico_basico.nc`` usando repositórios reais (SQLAlchemy +
SQLite in-memory) e comparamos os :class:`ResultadoIndice` persistidos
com o CSV baseline ``baseline_grade_basico.csv`` gerado pelo legado
``cordex_pr_freq_intensity.py``.

Gate bloqueante do Slice 6: rtol=1e-6, atol=1e-9.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.application.execucoes.processar_cenario import (
    ParametrosProcessamento,
    ProcessarCenarioCordex,
)
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.indices.calculadora import ParametrosIndices
from climate_risk.domain.indices.p95 import PeriodoBaseline
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray

FIXTURE_NC = (
    Path(__file__).resolve().parent.parent.parent
    / "fixtures"
    / "netcdf_mini"
    / "cordex_sintetico_basico.nc"
)
BASELINE_CSV = (
    Path(__file__).resolve().parent.parent.parent
    / "fixtures"
    / "baselines"
    / "sintetica"
    / "baseline_grade_basico.csv"
)

RTOL = 1e-6
ATOL = 1e-9


# Colunas do CSV que viram valores de índices (mesma ordem do header).
_COLUNAS_INDICES: tuple[tuple[str, str], ...] = (
    ("wet_days", "wet_days"),
    ("sdii", "sdii"),
    ("rx1day", "rx1day"),
    ("rx5day", "rx5day"),
    ("r20mm", "r20mm"),
    ("r50mm", "r50mm"),
    ("r95ptot_mm", "r95ptot_mm"),
    ("r95ptot_frac", "r95ptot_frac"),
)


def _chave(ano: int, lat: float, lon: float, nome: str) -> tuple[int, int, int, str]:
    """Chave de comparação. Arredondamos lat/lon para 4 casas para
    tolerar a representação ligeiramente diferente entre pandas/numpy e
    ``float`` nativo.
    """
    return (ano, round(lat * 1e4), round(lon * 1e4), nome)


def _ler_baseline() -> dict[tuple[int, int, int, str], float]:
    mapa: dict[tuple[int, int, int, str], float] = {}
    with BASELINE_CSV.open("r", encoding="utf-8") as f:
        leitor = csv.DictReader(f)
        for linha in leitor:
            ano = int(linha["year"])
            lat = float(linha["lat"])
            lon = float(linha["lon"])
            for nome_csv, nome_interno in _COLUNAS_INDICES:
                bruto = linha[nome_csv].strip()
                valor = float("nan") if bruto == "" else float(bruto)
                mapa[_chave(ano, lat, lon, nome_interno)] = valor
    return mapa


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.skipif(
    not BASELINE_CSV.exists(),
    reason="Baseline CSV sintético ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.asyncio
async def test_paridade_processar_cenario_vs_baseline(async_session: AsyncSession) -> None:
    repo_execucoes = SQLAlchemyRepositorioExecucoes(async_session)
    repo_resultados = SQLAlchemyRepositorioResultados(async_session)
    leitor = LeitorXarray()

    # Pré-cria a Execucao em ``pending`` para simular o que
    # ``CriarExecucaoCordex`` faria em produção.
    execucao = Execucao(
        id="exec_paridade_basico",
        cenario="rcp45",
        variavel="pr",
        arquivo_origem=str(FIXTURE_NC),
        tipo="grade_bbox",
        parametros={},
        status=StatusExecucao.PENDING,
        criado_em=utc_now(),
        concluido_em=None,
        job_id=None,
    )
    await repo_execucoes.salvar(execucao)

    caso = ProcessarCenarioCordex(
        leitor_netcdf=leitor,
        repositorio_execucoes=repo_execucoes,
        repositorio_resultados=repo_resultados,
    )
    params = ParametrosProcessamento(
        execucao_id=execucao.id,
        arquivo_nc=str(FIXTURE_NC),
        variavel="pr",
        bbox=None,
        parametros_indices=ParametrosIndices(freq_thr_mm=20.0, heavy_thresholds=(20.0, 50.0)),
        p95_baseline=PeriodoBaseline(2026, 2030),
        p95_wet_thr=1.0,
    )

    sumario = await caso.executar(params)
    await async_session.commit()

    # Execução finalizou em ``completed``.
    persistida = await repo_execucoes.buscar_por_id(execucao.id)
    assert persistida is not None
    assert persistida.status == StatusExecucao.COMPLETED
    assert persistida.concluido_em is not None

    # 100 celulas x 5 anos x 8 indices = 4000 linhas.
    assert sumario.total_celulas == 100
    assert sumario.total_anos == 5
    assert sumario.total_resultados == 100 * 5 * 8

    baseline = _ler_baseline()

    # Todos os resultados persistidos foram lidos (paginação segura com limit alto).
    resultados = await repo_resultados.listar(execucao_id=execucao.id, limit=10_000)
    assert len(resultados) == sumario.total_resultados

    divergencias: list[str] = []
    chaves_vistas: set[tuple[int, int, int, str]] = set()
    for r in resultados:
        chave = _chave(r.ano, r.lat, r.lon, r.nome_indice)
        chaves_vistas.add(chave)
        if chave not in baseline:
            divergencias.append(f"chave ausente no baseline: {chave}")
            continue
        valor_baseline = baseline[chave]
        valor_obtido = float("nan") if r.valor is None else r.valor
        if math.isnan(valor_baseline):
            if not math.isnan(valor_obtido):
                divergencias.append(f"{chave}: esperado NaN, obtido {valor_obtido}")
            continue
        if math.isnan(valor_obtido):
            divergencias.append(f"{chave}: esperado {valor_baseline}, obtido NaN")
            continue
        if not math.isclose(valor_obtido, valor_baseline, rel_tol=RTOL, abs_tol=ATOL):
            divergencias.append(f"{chave}: obtido={valor_obtido}, baseline={valor_baseline}")

    # Checagem simétrica: nenhuma chave do baseline pode ficar de fora.
    faltantes = set(baseline) - chaves_vistas
    if faltantes:
        divergencias.extend(f"chave do baseline não persistida: {c}" for c in sorted(faltantes))

    assert not divergencias, (
        f"Paridade quebrada em {len(divergencias)} linhas. Primeiras 10:\n"
        + "\n".join(divergencias[:10])
    )
