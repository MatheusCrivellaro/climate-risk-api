"""Testes e2e de ``POST /calculos/pontos`` (UC-03 síncrono — M1)."""

from __future__ import annotations

import csv
import math
import os
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from climate_risk.infrastructure.db.modelos import ExecucaoORM, ResultadoIndiceORM

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini"
FIXTURE_NC = FIXTURES / "cordex_sintetico_basico.nc"
PONTOS_CSV = FIXTURES / "pontos_fixos.csv"
LEGACY_SCRIPT = Path(__file__).resolve().parents[3] / "legacy" / "gera_pontos_fornecedores.py"

RTOL = 1e-6
ATOL = 1e-9


def _corpo_basico(
    *,
    pontos: list[dict[str, object]] | None = None,
    persistir: bool = False,
    arquivo_nc: str | None = None,
    variavel: str = "pr",
) -> dict[str, object]:
    return {
        "arquivo_nc": arquivo_nc if arquivo_nc is not None else str(FIXTURE_NC),
        "cenario": "rcp45",
        "variavel": variavel,
        "pontos": pontos
        if pontos is not None
        else [{"lat": -22.9, "lon": -46.5, "identificador": "PontoA"}],
        "parametros_indices": {
            "freq_thr_mm": 20.0,
            "p95_wet_thr": 1.0,
            "heavy20": 20.0,
            "heavy50": 50.0,
            "p95_baseline": {"inicio": 2026, "fim": 2030},
        },
        "persistir": persistir,
    }


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.asyncio
async def test_happy_path_sem_persistencia(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post("/calculos/pontos", json=_corpo_basico())

    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["cenario"] == "rcp45"
    assert corpo["variavel"] == "pr"
    assert corpo["execucao_id"] is None
    assert corpo["total_pontos"] == 1
    assert corpo["total_resultados"] >= 1
    assert all("indices" in linha for linha in corpo["resultados"])


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente.",
)
@pytest.mark.asyncio
async def test_persiste_execucao_e_resultados_no_banco(
    cliente_api: AsyncClient,
    async_sessionmaker_: async_sessionmaker[AsyncSession],
) -> None:
    resposta = await cliente_api.post("/calculos/pontos", json=_corpo_basico(persistir=True))

    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    execucao_id = corpo["execucao_id"]
    assert isinstance(execucao_id, str) and execucao_id.startswith("exec_")

    async with async_sessionmaker_() as sessao:
        execucao = (
            await sessao.execute(select(ExecucaoORM).where(ExecucaoORM.id == execucao_id))
        ).scalar_one()
        assert execucao.tipo == "pontos"
        assert execucao.status == "completed"

        resultados = (
            (
                await sessao.execute(
                    select(ResultadoIndiceORM).where(ResultadoIndiceORM.execucao_id == execucao_id)
                )
            )
            .scalars()
            .all()
        )
        # 1 ponto * 5 anos do fixture basico * 8 indices = 40 linhas.
        assert len(resultados) == 40
        nomes = {r.nome_indice for r in resultados}
        assert nomes == {
            "wet_days",
            "sdii",
            "rx1day",
            "rx5day",
            "r20mm",
            "r50mm",
            "r95ptot_mm",
            "r95ptot_frac",
        }


# NOTA: o teste ``test_excede_limite_sincrono_retorna_400`` (Slice 4) foi
# removido. A partir do Slice 7, lotes acima de ``sincrono_pontos_max``
# deixaram de retornar 400 e passaram a devolver 202 enfileirando um job.
# A cobertura do novo comportamento vive em
# ``tests/e2e/api/test_calculos_pontos_async.py``.


@pytest.mark.asyncio
async def test_arquivo_nc_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/calculos/pontos", json=_corpo_basico(arquivo_nc="/nao/existe.nc")
    )
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"].endswith("/arquivo-nc-nao-encontrado")
    assert corpo["status"] == 404
    assert "correlation_id" in corpo


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente.",
)
@pytest.mark.asyncio
async def test_variavel_ausente_retorna_422(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/calculos/pontos", json=_corpo_basico(variavel="variavel_inexistente")
    )
    assert resposta.status_code == 422
    corpo = resposta.json()
    assert corpo["type"].endswith("/variavel-ausente")


@pytest.mark.asyncio
async def test_payload_invalido_retorna_422_pydantic(cliente_api: AsyncClient) -> None:
    # Latitude fora do intervalo — o próprio Pydantic rejeita antes do caso de uso.
    resposta = await cliente_api.post(
        "/calculos/pontos",
        json=_corpo_basico(pontos=[{"lat": 999.0, "lon": -46.5}]),
    )
    assert resposta.status_code == 422  # FastAPI/Pydantic default (RequestValidationError)


@pytest.mark.skipif(
    not FIXTURE_NC.exists() or not LEGACY_SCRIPT.exists(),
    reason="Fixture sintética ou script legado ausente.",
)
@pytest.mark.asyncio
async def test_paridade_bit_a_bit_vs_legacy_via_subprocess(
    cliente_api: AsyncClient,
) -> None:
    """Teste 7 (gate do slice): paridade numérica com o legado.

    Pede ao endpoint o cálculo para os mesmos pontos que o legado processa
    via CSV e compara, índice-a-índice, para cada (ponto, ano). Tolerância
    ``rtol=1e-6, atol=1e-9`` por decisão do Slice 0 (baseline sintética).
    """
    pontos_in: list[dict[str, object]] = []
    with PONTOS_CSV.open(encoding="utf-8", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)
        for linha in leitor:
            pontos_in.append(
                {
                    "lat": float(linha["lat"]),
                    "lon": float(linha["lon"]),
                    "identificador": f"{linha['cidade']}/{linha['estado']}",
                }
            )

    # 1. Chamada à API.
    resposta = await cliente_api.post("/calculos/pontos", json=_corpo_basico(pontos=pontos_in))
    assert resposta.status_code == 200, resposta.text
    respostas_novas = resposta.json()["resultados"]

    # 2. Execução do script legado em subprocess.
    with tempfile.TemporaryDirectory() as tmpdir:
        out_csv = os.path.join(tmpdir, "legacy_out.csv")
        comando = [
            sys.executable,
            str(LEGACY_SCRIPT),
            "--glob",
            str(FIXTURE_NC),
            "--points-csv",
            str(PONTOS_CSV),
            "--out",
            out_csv,
            "--freq-thr-mm",
            "20.0",
            "--p95-wet-thr",
            "1.0",
            "--heavy20",
            "20.0",
            "--heavy50",
            "50.0",
            "--p95-baseline",
            "2026-2030",
        ]
        resultado_subprocess = subprocess.run(
            comando,
            capture_output=True,
            text=True,
            check=False,
            cwd=str(LEGACY_SCRIPT.parent.parent),
        )
        assert resultado_subprocess.returncode == 0, (
            f"Script legado falhou.\nstdout:\n{resultado_subprocess.stdout}\n"
            f"stderr:\n{resultado_subprocess.stderr}"
        )

        with open(out_csv, encoding="utf-8-sig", newline="") as arq:
            linhas_legadas = list(csv.DictReader(arq))

    # 3. Indexa ambos por (identificador/ponto+ano).
    def _chave_nova(linha: dict[str, object]) -> tuple[float, float, int]:
        return (
            round(float(linha["lat_input"]), 6),
            round(float(linha["lon_input"]), 6),
            int(linha["ano"]),
        )

    def _chave_legado(linha: dict[str, str]) -> tuple[float, float, int]:
        return (
            round(float(linha["lat_input"]), 6),
            round(float(linha["lon_input"]), 6),
            int(linha["year"]),
        )

    mapa_legado = {_chave_legado(linha): linha for linha in linhas_legadas}
    assert len(mapa_legado) == len(linhas_legadas), "Chaves duplicadas no CSV legado."
    assert len(respostas_novas) == len(linhas_legadas), (
        f"Divergência de cardinalidade: novo={len(respostas_novas)}, legacy={len(linhas_legadas)}."
    )

    pares_indice = [
        ("wet_days", "wet_days", True),
        ("sdii", "sdii", False),
        ("rx1day", "rx1day", False),
        ("rx5day", "rx5day", False),
        ("r20mm", "r20mm", True),
        ("r50mm", "r50mm", True),
        ("r95ptot_mm", "r95ptot_mm", False),
        ("r95ptot_frac", "r95ptot_frac", False),
    ]

    for linha in respostas_novas:
        chave = _chave_nova(linha)
        assert chave in mapa_legado, f"Linha sem correspondente no legado: {chave}"
        legado = mapa_legado[chave]
        # Grade escolhida deve coincidir.
        assert math.isclose(
            float(linha["lat_grid"]), float(legado["lat_grid"]), rel_tol=RTOL, abs_tol=ATOL
        )
        assert math.isclose(
            float(linha["lon_grid"]), float(legado["lon_grid"]), rel_tol=RTOL, abs_tol=ATOL
        )

        indices = linha["indices"]
        for nome_novo, nome_legado, inteiro in pares_indice:
            valor_novo = indices[nome_novo]
            valor_legado_str = legado[nome_legado]

            if valor_legado_str == "" or valor_legado_str.lower() == "nan":
                # Legado escreve vazio ou "nan" para NaN; novo devolve None.
                assert valor_novo is None, (
                    f"{nome_novo} esperado None, recebido {valor_novo} (chave={chave})"
                )
                continue

            if valor_novo is None:
                pytest.fail(
                    f"{nome_novo} esperado numérico ({valor_legado_str}), "
                    f"recebido None (chave={chave})"
                )

            if inteiro:
                assert int(valor_novo) == int(valor_legado_str), (
                    f"{nome_novo} (chave={chave}): novo={valor_novo}, legado={valor_legado_str}"
                )
            else:
                assert math.isclose(
                    float(valor_novo), float(valor_legado_str), rel_tol=RTOL, abs_tol=ATOL
                ), f"{nome_novo} (chave={chave}): novo={valor_novo}, legado={valor_legado_str}"


# Mantido apenas para silenciar linters que exigem ao menos um símbolo importado.
_: AsyncIterator[int] | None = None
