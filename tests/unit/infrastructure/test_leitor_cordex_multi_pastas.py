"""Testes do método ``abrir_de_pastas`` (Slice 17).

Usa as fixtures sintéticas existentes (``climatologia_multi/``) copiadas
para pastas temporárias. Cobre:

- Caso feliz com 1 arquivo por pasta (concatenação trivial).
- Concatenação de 2 arquivos no mesmo eixo temporal (split em metade).
- Pasta vazia → ``ErroPastaVazia``.
- Cenário divergente → ``ErroCenarioInconsistente``.
- Sobreposição de timestamps → mantém o primeiro e loga warning.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

from climate_risk.domain.excecoes import (
    ErroCenarioInconsistente,
    ErroPastaVazia,
)
from climate_risk.infrastructure.leitor_cordex_multi import LeitorCordexMultiVariavel

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "climatologia_multi"
FIX_PR = FIXTURES / "pr_sintetico.nc"
FIX_TAS = FIXTURES / "tas_sintetico.nc"
FIX_EVAP = FIXTURES / "evspsbl_sintetico.nc"


def _montar_pastas(tmp_path: Path) -> tuple[Path, Path, Path]:
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for pasta in (pasta_pr, pasta_tas, pasta_evap):
        pasta.mkdir(parents=True, exist_ok=True)
    shutil.copy2(FIX_PR, pasta_pr / "pr_2026.nc")
    shutil.copy2(FIX_TAS, pasta_tas / "tas_2026.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evap_2026.nc")
    return pasta_pr, pasta_tas, pasta_evap


def test_abre_de_pastas_com_um_arquivo_por_pasta(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _montar_pastas(tmp_path)

    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")

    dados.validar()
    assert dados.cenario == "rcp45"
    assert len(dados.tempo) == 10
    assert dados.precipitacao_diaria_mm.attrs["units"] == "mm/day"


def test_abre_de_pastas_concatena_dois_arquivos_no_tempo(tmp_path: Path) -> None:
    """Divide o fixture pr (10 dias) em duas metades e confere concatenação."""
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for pasta in (pasta_pr, pasta_tas, pasta_evap):
        pasta.mkdir(parents=True)

    with xr.open_dataset(FIX_PR) as ds_pr:
        ds_pr.isel(time=slice(0, 5)).to_netcdf(pasta_pr / "pr_parte1.nc")
        ds_pr.isel(time=slice(5, 10)).to_netcdf(pasta_pr / "pr_parte2.nc")
    shutil.copy2(FIX_TAS, pasta_tas / "tas.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evap.nc")

    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")
    assert len(dados.tempo) == 10
    assert dados.tempo.is_monotonic_increasing


def test_pasta_vazia_levanta_erro_pasta_vazia(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _montar_pastas(tmp_path)
    # Esvaziar pasta_evap.
    for arq in pasta_evap.glob("*.nc"):
        arq.unlink()

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroPastaVazia) as exc:
        leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")
    assert exc.value.variavel == "evspsbl"
    assert str(pasta_evap) in str(exc.value)


def test_pasta_inexistente_levanta_erro_pasta_vazia(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, _ = _montar_pastas(tmp_path)
    pasta_evap_inexistente = tmp_path / "nao_existe"

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroPastaVazia):
        leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap_inexistente, "rcp45")


def test_arquivo_com_cenario_divergente_levanta_erro(tmp_path: Path) -> None:
    """Atributo ``experiment_id`` divergente em um dos arquivos da pasta pr."""
    pasta_pr, pasta_tas, pasta_evap = _montar_pastas(tmp_path)

    arq_pr = pasta_pr / "pr_2026.nc"
    with xr.open_dataset(arq_pr) as ds:
        ds_carregado = ds.load()
    ds_carregado.attrs["experiment_id"] = "rcp85"
    arq_pr.unlink()
    ds_carregado.to_netcdf(arq_pr)
    ds_carregado.close()

    leitor = LeitorCordexMultiVariavel()
    with pytest.raises(ErroCenarioInconsistente) as exc:
        leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")
    assert "pr_2026.nc" in str(exc.value)


def test_sobreposicao_de_timestamps_mantem_primeiro(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Dois arquivos compartilhando o mesmo período: deduplica e loga warning."""
    pasta_pr = tmp_path / "pr"
    pasta_tas = tmp_path / "tas"
    pasta_evap = tmp_path / "evap"
    for pasta in (pasta_pr, pasta_tas, pasta_evap):
        pasta.mkdir(parents=True)

    # Dois arquivos pr cobrindo os MESMOS 10 dias (sobreposição total).
    shutil.copy2(FIX_PR, pasta_pr / "pr_v1.nc")
    shutil.copy2(FIX_PR, pasta_pr / "pr_v2.nc")
    shutil.copy2(FIX_TAS, pasta_tas / "tas.nc")
    shutil.copy2(FIX_EVAP, pasta_evap / "evap.nc")

    leitor = LeitorCordexMultiVariavel()
    with caplog.at_level("WARNING"):
        dados = leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")
    assert len(dados.tempo) == 10
    assert any("duplicados" in record.message for record in caplog.records)


def test_cenario_esperado_case_insensitive(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _montar_pastas(tmp_path)
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "RCP45")
    assert dados.cenario == "rcp45"


def test_tempo_resultante_eh_datetime_index(tmp_path: Path) -> None:
    pasta_pr, pasta_tas, pasta_evap = _montar_pastas(tmp_path)
    leitor = LeitorCordexMultiVariavel()
    dados = leitor.abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, "rcp45")
    assert isinstance(dados.tempo, pd.DatetimeIndex)
