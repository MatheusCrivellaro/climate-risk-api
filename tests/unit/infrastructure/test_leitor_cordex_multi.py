"""Testes do adapter :class:`LeitorCordexMultiVariavel` (Slice 13).

Usa os fixtures sintéticos em ``tests/fixtures/climatologia_multi/`` (gerados
por ``gerar_fixtures.py``). Os três arquivos partilham o cenário ``rcp45`` por
construção — testes que precisam de cenários divergentes reescrevem um deles
em ``tmp_path`` antes de instanciar o adapter.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.domain.excecoes import (
    ErroArquivoNCNaoEncontrado,
    ErroCenarioInconsistente,
    ErroLeituraNetCDF,
)
from climate_risk.infrastructure.leitor_cordex_multi import (
    LeitorCordexMultiVariavel,
    _converter_unidade_precipitacao,
    _converter_unidade_temperatura,
    _identificar_variavel_principal,
    _normalizar_calendario,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "climatologia_multi"
FIX_PR = FIXTURES / "pr_sintetico.nc"
FIX_TAS = FIXTURES / "tas_sintetico.nc"
FIX_EVAP = FIXTURES / "evspsbl_sintetico.nc"


def _copiar_fixture(origem: Path, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(origem, destino)


def test_abre_tres_arquivos_e_retorna_entidade_valida() -> None:
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir(FIX_PR, FIX_TAS, FIX_EVAP)

    dados.validar()
    assert dados.cenario == "rcp45"
    assert len(dados.tempo) == 10
    assert isinstance(dados.tempo, pd.DatetimeIndex)

    # Unidades convertidas — precipitação em mm/dia, temperatura em °C.
    assert dados.precipitacao_diaria_mm.attrs["units"] == "mm/day"
    assert dados.temperatura_diaria_c.attrs["units"] == "degC"
    assert dados.evaporacao_diaria_mm.attrs["units"] == "mm/day"


def test_precipitacao_e_evap_convertidas_para_mm_dia() -> None:
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir(FIX_PR, FIX_TAS, FIX_EVAP)

    # A fixture encapsula valores conhecidos no ponto central (índice 1, 1) em
    # mm/dia por construção — ver gerar_fixtures.py.
    pr_central = dados.precipitacao_diaria_mm.isel(lat=1, lon=1).values
    np.testing.assert_allclose(
        pr_central,
        np.array([0.5, 0.2, 5.0, 0.0, 0.1, 2.0, 0.3, 0.0, 10.0, 0.4]),
        rtol=1e-4,
    )


def test_temperatura_convertida_para_celsius_range_esperado() -> None:
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir(FIX_PR, FIX_TAS, FIX_EVAP)

    # Faixa construída: 22 a 37 °C no ponto central.
    tas_central = dados.temperatura_diaria_c.isel(lat=1, lon=1).values
    assert tas_central.min() >= 20.0
    assert tas_central.max() <= 40.0
    np.testing.assert_allclose(
        tas_central,
        np.array([28.0, 32.0, 31.5, 25.0, 33.0, 29.0, 35.0, 27.0, 30.5, 24.0]),
        rtol=1e-4,
    )


def test_calendario_noleap_convertido_sem_dias_spurios(tmp_path: Path) -> None:
    """``evspsbl`` em ``noleap`` vira DatetimeIndex gregoriano limpo."""
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir(FIX_PR, FIX_TAS, FIX_EVAP)

    # Nenhum dos 10 dias cai em 29/fev; todos os timestamps existem no
    # calendário gregoriano.
    for ts in dados.tempo:
        assert not (ts.month == 2 and ts.day == 29)
    assert dados.tempo[0] == pd.Timestamp("2026-01-01")
    assert dados.tempo[-1] == pd.Timestamp("2026-01-10")


def test_intersecao_temporal_quando_um_arquivo_tem_dia_a_mais(tmp_path: Path) -> None:
    """Adapter toma a interseção — um arquivo com 11 dias reduz para 10."""
    pr_estendido = tmp_path / "pr_extra.nc"
    ds = xr.open_dataset(FIX_PR)
    try:
        # Gera um dia extra para ``pr`` (11 dias vs 10 dos outros).
        tempo_novo = pd.date_range("2026-01-01", periods=11, freq="D")
        valores_extra = np.concatenate([ds["pr"].values, ds["pr"].values[-1:] + 1e-9], axis=0)
        ds_novo = xr.Dataset(
            data_vars={
                "pr": (("time", "lat", "lon"), valores_extra, {"units": "kg m-2 s-1"}),
            },
            coords={
                "time": tempo_novo,
                "lat": ds["lat"],
                "lon": ds["lon"],
            },
            attrs={"experiment_id": "rcp45"},
        )
        ds_novo.to_netcdf(pr_estendido, engine="netcdf4")
    finally:
        ds.close()

    dados = LeitorCordexMultiVariavel().abrir(pr_estendido, FIX_TAS, FIX_EVAP)
    assert len(dados.tempo) == 10


def test_cenario_divergente_levanta_erro(tmp_path: Path) -> None:
    """Reescreve o arquivo de ``tas`` com ``experiment_id = rcp85``."""
    tas_divergente = tmp_path / "tas_rcp85.nc"
    ds = xr.open_dataset(FIX_TAS)
    try:
        ds_novo = ds.copy()
        ds_novo.attrs["experiment_id"] = "rcp85"
        ds_novo.to_netcdf(tas_divergente, engine="netcdf4")
    finally:
        ds.close()

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroCenarioInconsistente) as info:
        leitor.abrir(FIX_PR, tas_divergente, FIX_EVAP)
    assert "rcp45" in str(info.value)
    assert "rcp85" in str(info.value)


def test_arquivo_ausente_levanta_erro(tmp_path: Path) -> None:
    inexistente = tmp_path / "nao_existe.nc"
    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroArquivoNCNaoEncontrado) as info:
        leitor.abrir(inexistente, FIX_TAS, FIX_EVAP)
    assert "nao_existe.nc" in str(info.value)


def test_variavel_principal_identifica_pr_ignorando_auxiliares(tmp_path: Path) -> None:
    """Arquivo com ``pr`` + ``time_bnds`` + ``height`` deve devolver 'pr'."""
    origem = xr.open_dataset(FIX_PR)
    try:
        # Adiciona variáveis auxiliares que o adapter deve ignorar.
        extra = origem.assign(
            time_bnds=(("time", "nv"), np.zeros((origem.sizes["time"], 2), dtype=np.float32)),
            height=((), np.float32(2.0)),
        )
        destino = tmp_path / "pr_com_auxiliares.nc"
        extra.to_netcdf(destino, engine="netcdf4")
    finally:
        origem.close()

    with xr.open_dataset(destino) as ds:
        assert _identificar_variavel_principal(str(destino), ds) == "pr"


def test_conversor_precipitacao_multiplica_por_86400() -> None:
    da = xr.DataArray(np.array([1e-5, 2e-5], dtype=np.float32), attrs={"units": "kg m-2 s-1"})
    convertido = _converter_unidade_precipitacao(da)
    np.testing.assert_allclose(convertido.values, [0.864, 1.728], rtol=1e-6)
    assert convertido.attrs["units"] == "mm/day"


def test_conversor_precipitacao_nao_duplica_se_ja_em_mm_dia() -> None:
    da = xr.DataArray(np.array([1.0, 3.0], dtype=np.float32), attrs={"units": "mm/day"})
    convertido = _converter_unidade_precipitacao(da)
    np.testing.assert_allclose(convertido.values, [1.0, 3.0], rtol=1e-6)


def test_conversor_temperatura_subtrai_273_15() -> None:
    da = xr.DataArray(np.array([273.15, 300.15], dtype=np.float32), attrs={"units": "K"})
    convertido = _converter_unidade_temperatura(da)
    np.testing.assert_allclose(convertido.values, [0.0, 27.0], rtol=1e-5)
    assert convertido.attrs["units"] == "degC"


def test_conversor_temperatura_mantem_se_ja_em_celsius() -> None:
    da = xr.DataArray(np.array([10.0, 30.0], dtype=np.float32), attrs={"units": "degC"})
    convertido = _converter_unidade_temperatura(da)
    np.testing.assert_allclose(convertido.values, [10.0, 30.0], rtol=1e-6)


def test_normalizar_calendario_converte_noleap_para_gregoriano() -> None:
    tempo = xr.date_range("2026-01-01", periods=5, freq="D", calendar="noleap", use_cftime=True)
    da = xr.DataArray(
        np.arange(5, dtype=np.float32),
        coords={"time": tempo},
        dims=("time",),
    )
    convertido = _normalizar_calendario(da)
    assert isinstance(convertido.indexes["time"], pd.DatetimeIndex)


def test_erro_leitura_para_arquivo_corrompido(tmp_path: Path) -> None:
    """Arquivo não-NetCDF deve levantar ``ErroLeituraNetCDF``."""
    corrompido = tmp_path / "pr_lixo.nc"
    corrompido.write_bytes(b"nao sou um NetCDF valido")
    # Usa cópias válidas para tas/evap.
    tas_copy = tmp_path / "tas.nc"
    evap_copy = tmp_path / "evap.nc"
    _copiar_fixture(FIX_TAS, tas_copy)
    _copiar_fixture(FIX_EVAP, evap_copy)

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroLeituraNetCDF):
        leitor.abrir(corrompido, tas_copy, evap_copy)
