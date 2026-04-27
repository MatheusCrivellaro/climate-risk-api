"""Testes e2e do browser de pastas ``GET /api/fs/listar`` (Slice 20.1)."""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_sem_env_var_retorna_503(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLIMATE_RISK_FS_RAIZ", raising=False)
    resposta = await cliente_api.get("/api/fs/listar")
    assert resposta.status_code == 503
    assert "CLIMATE_RISK_FS_RAIZ" in resposta.text


@pytest.mark.asyncio
async def test_lista_raiz_quando_env_var_setada(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "subpasta_a").mkdir()
    (tmp_path / "subpasta_b").mkdir()
    (tmp_path / "arquivo.nc").write_bytes(b"\x00")
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))

    resposta = await cliente_api.get("/api/fs/listar")
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["pasta_raiz"] == str(tmp_path.resolve())
    assert corpo["caminho_atual"] == str(tmp_path.resolve())
    assert corpo["pode_subir"] is False
    assert corpo["pasta_pai"] is None
    nomes_subpastas = [s["nome"] for s in corpo["subpastas"]]
    assert nomes_subpastas == ["subpasta_a", "subpasta_b"]
    nomes_arquivos = [a["nome"] for a in corpo["arquivos_nc"]]
    assert nomes_arquivos == ["arquivo.nc"]


@pytest.mark.asyncio
async def test_path_traversal_externo_retorna_400(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raiz = tmp_path / "raiz"
    raiz.mkdir()
    fora = tmp_path / "fora"
    fora.mkdir()
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(raiz))

    resposta = await cliente_api.get("/api/fs/listar", params={"caminho": str(fora)})
    assert resposta.status_code == 400
    assert "fora da pasta raiz" in resposta.text.lower()


@pytest.mark.asyncio
async def test_path_traversal_via_dotdot_retorna_400(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raiz = tmp_path / "raiz"
    raiz.mkdir()
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(raiz))

    # `raiz/../` resolve para tmp_path, que está fora da raiz.
    resposta = await cliente_api.get("/api/fs/listar", params={"caminho": str(raiz / "..")})
    assert resposta.status_code == 400


@pytest.mark.asyncio
async def test_caminho_inexistente_retorna_404(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))
    resposta = await cliente_api.get(
        "/api/fs/listar", params={"caminho": str(tmp_path / "nao-existe")}
    )
    assert resposta.status_code == 404


@pytest.mark.asyncio
async def test_navegacao_para_subpasta_marca_pode_subir(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sub = tmp_path / "sub"
    sub.mkdir()
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))

    resposta = await cliente_api.get("/api/fs/listar", params={"caminho": str(sub)})
    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["pode_subir"] is True
    assert corpo["pasta_pai"] == str(tmp_path.resolve())
    assert corpo["caminho_relativo_raiz"] == "sub"


@pytest.mark.asyncio
async def test_deteccao_cenario_em_nomes_de_arquivo(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "pr_day_BR_rcp45_2026-2035.nc").write_bytes(b"")
    (tmp_path / "tas_day_BR_rcp85_2026-2035.nc").write_bytes(b"")
    (tmp_path / "evspsbl_day_BR_ssp245_2026-2035.nc").write_bytes(b"")
    (tmp_path / "sem_cenario.nc").write_bytes(b"")
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))

    resposta = await cliente_api.get("/api/fs/listar")
    assert resposta.status_code == 200
    arquivos = {a["nome"]: a["cenario_detectado"] for a in resposta.json()["arquivos_nc"]}
    assert arquivos["pr_day_BR_rcp45_2026-2035.nc"] == "rcp45"
    assert arquivos["tas_day_BR_rcp85_2026-2035.nc"] == "rcp85"
    assert arquivos["evspsbl_day_BR_ssp245_2026-2035.nc"] == "ssp245"
    assert arquivos["sem_cenario.nc"] is None


@pytest.mark.asyncio
async def test_contagem_de_nc_por_subpasta(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pasta_a = tmp_path / "a"
    pasta_a.mkdir()
    (pasta_a / "x.nc").write_bytes(b"")
    (pasta_a / "y.nc").write_bytes(b"")
    (pasta_a / "ignorar.txt").write_bytes(b"")

    pasta_b = tmp_path / "b"
    pasta_b.mkdir()

    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))

    resposta = await cliente_api.get("/api/fs/listar")
    assert resposta.status_code == 200
    contagens = {s["nome"]: s["quantidade_nc"] for s in resposta.json()["subpastas"]}
    assert contagens == {"a": 2, "b": 0}


@pytest.mark.asyncio
async def test_caminho_para_arquivo_retorna_400(
    cliente_api: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    arquivo = tmp_path / "x.nc"
    arquivo.write_bytes(b"")
    monkeypatch.setenv("CLIMATE_RISK_FS_RAIZ", str(tmp_path))
    resposta = await cliente_api.get("/api/fs/listar", params={"caminho": str(arquivo)})
    assert resposta.status_code == 400
