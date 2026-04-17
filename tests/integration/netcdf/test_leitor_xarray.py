"""Testes de integração do adaptador :class:`LeitorXarray`.

Usa os fixtures sintéticos em ``tests/fixtures/netcdf_mini/``:

- ``cordex_sintetico_basico.nc``: calendário padrão, grade 1D (lat, lon).
- ``cordex_sintetico_cftime.nc``: calendário ``360_day``, grade 2D (y, x).

Os testes de paridade reimplementam inline as funções do legado
(``legacy/gera_pontos_fornecedores.py``) — NÃO importam de ``legacy/``,
seguindo o padrão já estabelecido em ``tests/integration/test_paridade_legacy.py``.
"""

from __future__ import annotations

import os
import re
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr

from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos
from climate_risk.domain.espacial.grade import coords_to_2d
from climate_risk.domain.espacial.longitude import ensure_lon_negpos180
from climate_risk.domain.excecoes import (
    ErroArquivoNCNaoEncontrado,
    ErroVariavelAusente,
)
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray

FIXTURES = Path(__file__).resolve().parent.parent.parent / "fixtures" / "netcdf_mini"
FIXTURE_BASICO = FIXTURES / "cordex_sintetico_basico.nc"
FIXTURE_CFTIME = FIXTURES / "cordex_sintetico_cftime.nc"

RTOL = 1e-6
ATOL = 1e-9


# ---------------------------------------------------------------------------
# Helpers legados copiados de gera_pontos_fornecedores.py (fonte primária
# ADR-001). Cópia inline evita dependência estrutural com ``legacy/``.
# ---------------------------------------------------------------------------


_SCENARIO_RE_LEGACY = re.compile(r"(rcp\d{2}|ssp\d{3})", re.IGNORECASE)


def _legacy_infer_scenario(path: str, ds: xr.Dataset) -> str:
    name = os.path.basename(path)
    match = _SCENARIO_RE_LEGACY.search(name)
    if match:
        return match.group(1).lower()
    for k in ("experiment_id", "scenario", "experiment"):
        v = str(ds.attrs.get(k, "")).strip().lower()
        if v:
            return v
    return "unknown"


def _legacy_open_nc_multi(path: str) -> xr.Dataset:
    last_err: Exception | None = None
    for eng in ("netcdf4", "h5netcdf", "scipy", None):
        for use_cftime in (True, False):
            try:
                kwargs: dict[str, Any] = {
                    "engine": eng,
                    "decode_times": True,
                    "mask_and_scale": True,
                }
                if use_cftime:
                    kwargs["use_cftime"] = True
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", FutureWarning)
                    ds = xr.open_dataset(path, **kwargs)
                _ = list(ds.sizes)
                return ds
            except TypeError as err:
                if "unexpected keyword argument 'use_cftime'" in str(err):
                    continue
                last_err = err
            except Exception as err:
                last_err = err
    raise last_err  # type: ignore[misc]


def _legacy_guess_latlon_vars(ds: xr.Dataset) -> tuple[str, str]:
    for la in ("lat", "latitude", "y"):
        for lo in ("lon", "longitude", "x"):
            if la in ds.coords and lo in ds.coords:
                return la, lo
            if la in ds.variables and lo in ds.variables:
                return la, lo
    raise ValueError("lat/lon ausentes")


def _legacy_convert_pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    units = (da.attrs.get("units", "") or "").lower()
    vmax = float(da.max())
    if (
        ("kg m-2 s-1" in units)
        or ("kg m^-2 s^-1" in units)
        or ("mm s-1" in units)
        or ("mm/s" in units)
        or vmax < 5.0
    ):
        da = da * 86400.0
    da.attrs["units"] = "mm/day"
    return da


def _ler_com_legacy(caminho: Path) -> dict[str, np.ndarray | str]:
    """Replica a leitura do legado e retorna arrays de comparação."""
    ds = _legacy_open_nc_multi(str(caminho))
    try:
        pr = ds["pr"]
        pr = _legacy_convert_pr_to_mm_per_day(pr)
        lat_name, lon_name = _legacy_guess_latlon_vars(ds)
        lat_vals = np.asarray(ds[lat_name].values)
        lon_vals = np.asarray(ds[lon_name].values)
        lat2d, lon2d = coords_to_2d(lat_vals, lon_vals)
        lon2d = ensure_lon_negpos180(lon2d)
        anos = np.asarray(pr["time"].dt.year.values, dtype=np.int64)
        dados = np.asarray(pr.values)
        cenario = _legacy_infer_scenario(str(caminho), ds)
        return {
            "dados": dados,
            "lat_2d": np.asarray(lat2d),
            "lon_2d": np.asarray(lon2d),
            "anos": anos,
            "cenario": cenario,
        }
    finally:
        ds.close()


# ---------------------------------------------------------------------------
# Testes do contrato público (todos ``async``).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not FIXTURE_BASICO.exists(),
    reason="Fixture sintética básica ausente — rode scripts/gerar_baseline_sintetica.py",
)
async def test_abrir_arquivo_basico_retorna_dados_completos() -> None:
    leitor = LeitorXarray()
    dados = await leitor.abrir(str(FIXTURE_BASICO), "pr")

    assert isinstance(dados, DadosClimaticos)
    assert dados.dados_diarios.ndim == 3
    assert dados.lat_2d.ndim == 2
    assert dados.lon_2d.ndim == 2
    assert dados.dados_diarios.shape[1:] == dados.lat_2d.shape == dados.lon_2d.shape
    assert float(dados.lon_2d.min()) >= -180.0
    assert float(dados.lon_2d.max()) <= 180.0
    assert np.issubdtype(dados.anos.dtype, np.integer)
    assert isinstance(dados.conversao_unidade_aplicada, bool)
    assert isinstance(dados.cenario, str) and dados.cenario != ""
    assert dados.variavel == "pr"
    assert dados.arquivo_origem == str(FIXTURE_BASICO)


@pytest.mark.skipif(
    not FIXTURE_CFTIME.exists(),
    reason="Fixture cftime ausente — rode scripts/gerar_baseline_sintetica.py",
)
async def test_abrir_arquivo_cftime_360_day() -> None:
    """Confirma que o bug de calendário 360_day está corrigido (ADR-008).

    O legado ``cordex_pr_freq_intensity.py`` falha em alguns pontos com
    calendário ``360_day`` porque não força ``use_cftime=True``; o leitor
    novo tenta ``use_cftime=True`` primeiro.
    """
    leitor = LeitorXarray()
    dados = await leitor.abrir(str(FIXTURE_CFTIME), "pr")

    assert np.issubdtype(dados.anos.dtype, np.integer)
    assert dados.anos.size > 0
    assert dados.anos.min() >= 1900  # sanidade
    assert "360_day" in dados.calendario
    # 360 dias por n anos => multiplo exato de 360 quando calendario e 360_day.
    assert dados.dados_diarios.shape[0] % 360 == 0


async def test_abrir_arquivo_inexistente_levanta_erro() -> None:
    leitor = LeitorXarray()
    with pytest.raises(ErroArquivoNCNaoEncontrado) as exc_info:
        await leitor.abrir("/tmp/nao_existe_climate_risk.nc", "pr")
    assert exc_info.value.caminho == "/tmp/nao_existe_climate_risk.nc"


@pytest.mark.skipif(
    not FIXTURE_BASICO.exists(),
    reason="Fixture sintética básica ausente",
)
async def test_abrir_variavel_inexistente_levanta_erro() -> None:
    leitor = LeitorXarray()
    with pytest.raises(ErroVariavelAusente) as exc_info:
        await leitor.abrir(str(FIXTURE_BASICO), "nao_existe")
    assert exc_info.value.variavel == "nao_existe"


@pytest.mark.skipif(
    not FIXTURE_BASICO.exists(),
    reason="Fixture sintética básica ausente",
)
async def test_conversao_unidade_e_aplicada() -> None:
    leitor = LeitorXarray()
    dados = await leitor.abrir(str(FIXTURE_BASICO), "pr")

    # Fixture básica declara ``kg m-2 s-1``; heurística legada converte.
    assert dados.unidade_original.lower().startswith("kg")
    assert dados.conversao_unidade_aplicada is True
    # Escala: mm/dia típicos para ``pr`` ficam em O(1)..O(200);
    # valores em ``kg m-2 s-1`` brutos seriam O(1e-4).
    assert 0.0 <= float(dados.dados_diarios.max()) < 500.0
    assert float(dados.dados_diarios.max()) > 1.0


@pytest.mark.skipif(
    not FIXTURE_BASICO.exists(),
    reason="Fixture sintética básica ausente",
)
async def test_paridade_com_legacy_em_fixture_basica() -> None:
    leitor = LeitorXarray()
    novo = await leitor.abrir(str(FIXTURE_BASICO), "pr")
    legado = _ler_com_legacy(FIXTURE_BASICO)

    np.testing.assert_allclose(novo.dados_diarios, legado["dados"], rtol=RTOL, atol=ATOL)
    np.testing.assert_allclose(novo.lat_2d, legado["lat_2d"], rtol=RTOL, atol=ATOL)
    np.testing.assert_allclose(novo.lon_2d, legado["lon_2d"], rtol=RTOL, atol=ATOL)
    np.testing.assert_array_equal(novo.anos, legado["anos"])
    assert novo.cenario == legado["cenario"]


@pytest.mark.skipif(
    not FIXTURE_CFTIME.exists(),
    reason="Fixture cftime ausente",
)
async def test_paridade_com_legacy_em_fixture_cftime() -> None:
    """Paridade usando APENAS o legado de ``gera_pontos_fornecedores.py``.

    ``cordex_pr_freq_intensity.py`` tem bug com ``360_day`` (ADR-008) —
    não é usado como referência aqui.
    """
    leitor = LeitorXarray()
    novo = await leitor.abrir(str(FIXTURE_CFTIME), "pr")
    legado = _ler_com_legacy(FIXTURE_CFTIME)

    np.testing.assert_allclose(novo.dados_diarios, legado["dados"], rtol=RTOL, atol=ATOL)
    np.testing.assert_allclose(novo.lat_2d, legado["lat_2d"], rtol=RTOL, atol=ATOL)
    np.testing.assert_allclose(novo.lon_2d, legado["lon_2d"], rtol=RTOL, atol=ATOL)
    np.testing.assert_array_equal(novo.anos, legado["anos"])
    assert novo.cenario == legado["cenario"]


# ---------------------------------------------------------------------------
# Cobertura de caminhos de erro sem dependência de fixture: variável sem
# dimensão ``time`` e coordenadas lat/lon ausentes. Gera um ``.nc`` mínimo
# em ``tmp_path``.
# ---------------------------------------------------------------------------


def _escrever_nc(tmp_path: Path, builder: Callable[[], xr.Dataset], nome: str) -> Path:
    destino = tmp_path / nome
    ds = builder()
    try:
        ds.to_netcdf(destino)
    finally:
        ds.close()
    return destino


async def test_variavel_sem_dimensao_time_levanta_erro(tmp_path: Path) -> None:
    from climate_risk.domain.excecoes import ErroDimensaoTempoAusente

    def _sem_time() -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "pr": xr.DataArray(
                    np.zeros((3, 3), dtype=np.float32),
                    dims=("lat", "lon"),
                    attrs={"units": "mm/day"},
                ),
            },
            coords={
                "lat": np.linspace(-10.0, 10.0, 3),
                "lon": np.linspace(-50.0, -30.0, 3),
            },
        )

    caminho = _escrever_nc(tmp_path, _sem_time, "sem_time.nc")

    leitor = LeitorXarray()
    with pytest.raises(ErroDimensaoTempoAusente) as exc_info:
        await leitor.abrir(str(caminho), "pr")
    assert exc_info.value.variavel == "pr"


async def test_sem_coordenadas_latlon_levanta_erro(tmp_path: Path) -> None:
    from climate_risk.domain.excecoes import ErroCoordenadasLatLonAusentes

    def _sem_latlon() -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "pr": xr.DataArray(
                    np.zeros((2, 3, 3), dtype=np.float32),
                    dims=("time", "a", "b"),
                    attrs={"units": "mm/day"},
                ),
            },
            coords={
                "time": np.array(
                    ["2026-01-01", "2026-01-02"],
                    dtype="datetime64[ns]",
                ),
                "a": np.arange(3),
                "b": np.arange(3),
            },
        )

    caminho = _escrever_nc(tmp_path, _sem_latlon, "sem_latlon.nc")

    leitor = LeitorXarray()
    with pytest.raises(ErroCoordenadasLatLonAusentes):
        await leitor.abrir(str(caminho), "pr")


async def test_copiar_para_tmp_abre_copia_e_libera_tempdir(tmp_path: Path) -> None:
    """Com ``copiar_para_tmp=True`` o leitor opera sobre cópia e limpa após fechar."""
    leitor = LeitorXarray(copiar_para_tmp=True)
    dados = await leitor.abrir(str(FIXTURE_BASICO), "pr")
    assert dados.dados_diarios.ndim == 3
    # Diretórios ``nc_copy_*`` devem ter sido removidos após o retorno.
    restos = list(Path(tmp_path.root).glob("nc_copy_*"))
    assert restos == [] or all(not r.exists() for r in restos)


async def test_arquivo_corrompido_levanta_erro_leitura(tmp_path: Path) -> None:
    """Um ``.nc`` com bytes inválidos falha em todas as engines e vira ErroLeituraNetCDF."""
    from climate_risk.domain.excecoes import ErroLeituraNetCDF

    caminho = tmp_path / "corrompido.nc"
    caminho.write_bytes(b"isto nao e um netcdf valido")

    leitor = LeitorXarray()
    with pytest.raises(ErroLeituraNetCDF):
        await leitor.abrir(str(caminho), "pr")


async def test_cenario_desconhecido_retorna_unknown(tmp_path: Path) -> None:
    """Sem cenário no nome e sem atributo global, ``cenario == 'unknown'``."""

    def _sem_cenario() -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "pr": xr.DataArray(
                    np.ones((2, 3, 3), dtype=np.float32) * 30.0,
                    dims=("time", "lat", "lon"),
                    attrs={"units": "mm/day"},
                ),
            },
            coords={
                "time": np.array(
                    ["2026-01-01", "2026-01-02"],
                    dtype="datetime64[ns]",
                ),
                "lat": np.linspace(-10.0, 10.0, 3),
                "lon": np.linspace(-50.0, -30.0, 3),
            },
        )

    caminho = _escrever_nc(tmp_path, _sem_cenario, "arquivo_generico.nc")

    leitor = LeitorXarray()
    dados = await leitor.abrir(str(caminho), "pr")
    assert dados.cenario == "unknown"


async def test_cenario_inferido_do_nome_do_arquivo(tmp_path: Path) -> None:
    def _com_rcp_no_nome() -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "pr": xr.DataArray(
                    np.ones((2, 3, 3), dtype=np.float32) * 50.0,
                    dims=("time", "lat", "lon"),
                    attrs={"units": "mm/day"},
                ),
            },
            coords={
                "time": np.array(
                    ["2026-01-01", "2026-01-02"],
                    dtype="datetime64[ns]",
                ),
                "lat": np.linspace(-10.0, 10.0, 3),
                "lon": np.linspace(-50.0, -30.0, 3),
            },
            attrs={"experiment_id": "historical"},
        )

    caminho = _escrever_nc(tmp_path, _com_rcp_no_nome, "cenario_rcp85_teste.nc")

    leitor = LeitorXarray()
    dados = await leitor.abrir(str(caminho), "pr")
    # Regex do nome tem prioridade sobre ``experiment_id``.
    assert dados.cenario == "rcp85"
    assert dados.conversao_unidade_aplicada is False
