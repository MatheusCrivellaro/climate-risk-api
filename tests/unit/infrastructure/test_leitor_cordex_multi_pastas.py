"""Testes do método ``abrir_de_pastas`` do leitor multi-variável (Slice 17)."""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.domain.excecoes import (
    ErroCenarioInconsistente,
    ErroLeituraNetCDF,
    ErroPastaVazia,
)
from climate_risk.infrastructure.leitor_cordex_multi import LeitorCordexMultiVariavel

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "climatologia_multi"
FIX_PR = FIXTURES / "pr_sintetico.nc"
FIX_TAS = FIXTURES / "tas_sintetico.nc"
FIX_EVAP = FIXTURES / "evspsbl_sintetico.nc"


def _criar_pastas_validas(tmp_path: Path) -> tuple[Path, Path, Path]:
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    shutil.copy2(FIX_PR, pasta_pr / "pr_sintetico.nc")
    shutil.copy2(FIX_TAS, pasta_tas / "tas_sintetico.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evspsbl_sintetico.nc")
    return pasta_pr, pasta_tas, pasta_evap


def test_abrir_de_pastas_com_um_arquivo_em_cada(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _criar_pastas_validas(tmp_path)
    leitor = LeitorCordexMultiVariavel()

    dados = leitor.abrir_de_pastas(
        pasta_pr=pasta_pr,
        pasta_tas=pasta_tas,
        pasta_evap=pasta_evap,
        cenario_esperado="rcp45",
    )

    dados.validar()
    assert dados.cenario == "rcp45"
    assert len(dados.tempo) == 10
    assert isinstance(dados.tempo, pd.DatetimeIndex)


def test_abrir_de_pastas_concatena_dois_arquivos_no_tempo(tmp_path: Path) -> None:
    """Dois ``.nc`` na pasta de pr (5 dias cada) → 10 dias concatenados."""
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    shutil.copy2(FIX_TAS, pasta_tas / "tas_sintetico.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evspsbl_sintetico.nc")

    ds = xr.open_dataset(FIX_PR)
    try:
        primeiro = ds.isel(time=slice(0, 5))
        segundo = ds.isel(time=slice(5, 10))
        primeiro.to_netcdf(pasta_pr / "pr_parte1.nc", engine="netcdf4")
        segundo.to_netcdf(pasta_pr / "pr_parte2.nc", engine="netcdf4")
    finally:
        ds.close()

    dados = LeitorCordexMultiVariavel().abrir_de_pastas(
        pasta_pr=pasta_pr,
        pasta_tas=pasta_tas,
        pasta_evap=pasta_evap,
        cenario_esperado="rcp45",
    )
    assert len(dados.tempo) == 10
    assert dados.tempo[0] == pd.Timestamp("2026-01-01")
    assert dados.tempo[-1] == pd.Timestamp("2026-01-10")


def test_pasta_vazia_levanta_erro(tmp_path: Path) -> None:
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    # tas e evap populados, pr vazia
    shutil.copy2(FIX_TAS, pasta_tas / "tas_sintetico.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evspsbl_sintetico.nc")

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroPastaVazia) as info:
        leitor.abrir_de_pastas(
            pasta_pr=pasta_pr,
            pasta_tas=pasta_tas,
            pasta_evap=pasta_evap,
            cenario_esperado="rcp45",
        )
    assert "precipitacao" in str(info.value)
    assert str(pasta_pr) in str(info.value)


def test_arquivo_com_cenario_errado_levanta_erro(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _criar_pastas_validas(tmp_path)
    # reescreve o arquivo de tas com experiment_id divergente
    arquivo_tas = next(pasta_tas.glob("*.nc"))
    arquivo_tas.unlink()
    ds = xr.open_dataset(FIX_TAS)
    try:
        ds_novo = ds.copy()
        ds_novo.attrs["experiment_id"] = "rcp85"
        ds_novo.to_netcdf(arquivo_tas, engine="netcdf4")
    finally:
        ds.close()

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroCenarioInconsistente) as info:
        leitor.abrir_de_pastas(
            pasta_pr=pasta_pr,
            pasta_tas=pasta_tas,
            pasta_evap=pasta_evap,
            cenario_esperado="rcp45",
        )
    assert "rcp85" in str(info.value)


def test_intersecao_temporal_vazia_levanta_erro(tmp_path: Path) -> None:
    """pr no intervalo 2026 e tas/evap em 2030 → interseção vazia."""
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    shutil.copy2(FIX_PR, pasta_pr / "pr_sintetico.nc")

    # Gera tas e evap deslocados para 2030 (não há sobreposição com pr)
    ds_tas = xr.open_dataset(FIX_TAS)
    try:
        tempo_2030 = pd.date_range("2030-01-01", periods=10, freq="D")
        ds_novo = xr.Dataset(
            data_vars={
                "tas": (
                    ("time", "lat", "lon"),
                    ds_tas["tas"].values,
                    {"units": "K"},
                ),
            },
            coords={"time": tempo_2030, "lat": ds_tas["lat"], "lon": ds_tas["lon"]},
            attrs={"experiment_id": "rcp45"},
        )
        ds_novo.to_netcdf(pasta_tas / "tas_2030.nc", engine="netcdf4")
    finally:
        ds_tas.close()

    ds_evap = xr.open_dataset(FIX_EVAP)
    try:
        tempo_2030_evap = pd.date_range("2030-01-01", periods=10, freq="D")
        ds_novo = xr.Dataset(
            data_vars={
                "evspsbl": (
                    ("time", "lat", "lon"),
                    ds_evap["evspsbl"].values,
                    {"units": "kg m-2 s-1"},
                ),
            },
            coords={
                "time": tempo_2030_evap,
                "lat": ds_evap["lat"],
                "lon": ds_evap["lon"],
            },
            attrs={"experiment_id": "rcp45"},
        )
        ds_novo.to_netcdf(pasta_evap / "evap_2030.nc", engine="netcdf4")
    finally:
        ds_evap.close()

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroLeituraNetCDF) as info:
        leitor.abrir_de_pastas(
            pasta_pr=pasta_pr,
            pasta_tas=pasta_tas,
            pasta_evap=pasta_evap,
            cenario_esperado="rcp45",
        )
    assert "interseção temporal vazia" in str(info.value)


def test_timestamps_duplicados_sao_deduplicados(tmp_path: Path) -> None:
    """Dois arquivos de pr com sobreposição parcial → primeira ocorrência mantida."""
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for p in (pasta_pr, pasta_tas, pasta_evap):
        p.mkdir()
    shutil.copy2(FIX_TAS, pasta_tas / "tas_sintetico.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evspsbl_sintetico.nc")

    ds = xr.open_dataset(FIX_PR)
    try:
        # Duas cópias com sobreposição nos dias 5-9 (primeira tem 0-9, segunda 5-14
        # mas como FIX só tem 10 dias, vamos usar 0-7 e 5-9 = sobreposição em 5-7)
        primeiro = ds.isel(time=slice(0, 8))
        segundo = ds.isel(time=slice(5, 10))
        # multiplica valores do segundo para distinguir
        segundo = segundo.copy()
        segundo["pr"] = segundo["pr"] * 2.0
        primeiro.to_netcdf(pasta_pr / "pr_a.nc", engine="netcdf4")
        segundo.to_netcdf(pasta_pr / "pr_b.nc", engine="netcdf4")
    finally:
        ds.close()

    dados = LeitorCordexMultiVariavel().abrir_de_pastas(
        pasta_pr=pasta_pr,
        pasta_tas=pasta_tas,
        pasta_evap=pasta_evap,
        cenario_esperado="rcp45",
    )
    # 10 dias únicos (0..9) — sem duplicatas mesmo com sobreposição
    assert len(dados.tempo) == 10
    # O dia 5 deve manter o valor da primeira ocorrência (sem multiplicação por 2)
    pr_dia_5 = dados.precipitacao_diaria_mm.sel(time="2026-01-06").values
    # Esperado pelo primeiro arquivo (não pelo segundo, que foi *2):
    # série central original [0.5, 0.2, 5.0, 0.0, 0.1, 2.0, ...] → dia 5 (idx 5) = 2.0
    np.testing.assert_allclose(pr_dia_5[1, 1], 2.0, rtol=1e-4)
