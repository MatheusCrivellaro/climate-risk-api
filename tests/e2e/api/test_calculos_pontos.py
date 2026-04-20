"""Testes e2e de ``POST /calculos/pontos`` (UC-03 síncrono — M1).

O endpoint síncrono é **puro**: apenas calcula e devolve os resultados.
A persistência vive no fluxo assíncrono (202 / worker) e em
``POST /execucoes`` — testes daqueles caminhos cobrem o banco.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import AsyncClient

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "netcdf_mini"
FIXTURE_NC = FIXTURES / "cordex_sintetico_basico.nc"


def _corpo_basico(
    *,
    pontos: list[dict[str, object]] | None = None,
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
    }


@pytest.mark.skipif(
    not FIXTURE_NC.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
@pytest.mark.asyncio
async def test_happy_path_sem_persistencia(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post("/api/calculos/pontos", json=_corpo_basico())

    assert resposta.status_code == 200, resposta.text
    corpo = resposta.json()
    assert corpo["cenario"] == "rcp45"
    assert corpo["variavel"] == "pr"
    assert corpo["total_pontos"] == 1
    assert corpo["total_resultados"] >= 1
    assert all("indices" in linha for linha in corpo["resultados"])


# NOTA: o teste ``test_excede_limite_sincrono_retorna_400`` (Slice 4) foi
# removido. A partir do Slice 7, lotes acima de ``sincrono_pontos_max``
# deixaram de retornar 400 e passaram a devolver 202 enfileirando um job.
# A cobertura do novo comportamento vive em
# ``tests/e2e/api/test_calculos_pontos_async.py``.
#
# NOTA: o teste ``test_persiste_execucao_e_resultados_no_banco`` foi
# removido na Slice 12 junto com a reversão da DT-001: o endpoint
# síncrono voltou a ser puro e não persiste mais. A persistência segue
# coberta pelos testes do fluxo assíncrono (worker) e de ``POST /execucoes``.


@pytest.mark.asyncio
async def test_arquivo_nc_inexistente_retorna_404(cliente_api: AsyncClient) -> None:
    resposta = await cliente_api.post(
        "/api/calculos/pontos", json=_corpo_basico(arquivo_nc="/nao/existe.nc")
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
        "/api/calculos/pontos", json=_corpo_basico(variavel="variavel_inexistente")
    )
    assert resposta.status_code == 422
    corpo = resposta.json()
    assert corpo["type"].endswith("/variavel-ausente")


@pytest.mark.asyncio
async def test_payload_invalido_retorna_422_pydantic(cliente_api: AsyncClient) -> None:
    # Latitude fora do intervalo — o próprio Pydantic rejeita antes do caso de uso.
    resposta = await cliente_api.post(
        "/api/calculos/pontos",
        json=_corpo_basico(pontos=[{"lat": 999.0, "lon": -46.5}]),
    )
    assert resposta.status_code == 422  # FastAPI/Pydantic default (RequestValidationError)


# NOTA: o teste ``test_paridade_bit_a_bit_vs_legacy_via_subprocess`` foi
# removido na Slice 12 com a remoção do diretório ``legacy/``. A paridade
# numérica bit-a-bit foi validada no Marco M4 e continua coberta em
# ``tests/integration/test_paridade_legacy.py``, que reimplementa as
# funções legadas inline e não depende do diretório removido.
