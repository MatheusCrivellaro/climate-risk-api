"""Gera arquivos .nc sintéticos e a baseline de regressão.

Script one-shot executado manualmente durante o bootstrap do projeto
(``uv run python scripts/gerar_baseline_sintetica.py``). Gera dois
``.nc`` em ``tests/fixtures/netcdf_mini/`` e, originalmente, executava
os scripts legados ``cordex_pr_freq_intensity.py`` e
``gera_pontos_fornecedores.py`` contra eles para congelar os CSVs em
``tests/fixtures/baselines/sintetica/``.

O código legado foi removido na Slice 12 após a paridade numérica
bit-a-bit ter sido validada (Marco M4). Os CSVs e ``.nc`` ainda
vivem em ``tests/fixtures/`` e continuam servindo como fonte de
verdade para os testes; rodar este script sem o diretório ``legacy/``
falhará ao chamar os subprocessos — é o comportamento esperado.
"""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import numpy as np
import xarray as xr

RAIZ = Path(__file__).resolve().parent.parent
DIR_NETCDF = RAIZ / "tests" / "fixtures" / "netcdf_mini"
DIR_BASELINES = RAIZ / "tests" / "fixtures" / "baselines" / "sintetica"
LEGACY = RAIZ / "legacy"


def _gerar_basico(destino: Path) -> None:
    """Grade 10x10, 5 anos, calendário padrão, lat/lon 1D, ``pr`` em kg m-2 s-1."""
    rng = np.random.default_rng(seed=42)
    ny, nx = 10, 10
    lat = np.linspace(-23.0, -20.0, ny, dtype=np.float32)
    lon = np.linspace(-47.0, -44.0, nx, dtype=np.float32)
    tempo = xr.date_range(
        "2026-01-01", "2030-12-31", freq="D", calendar="standard", use_cftime=False
    )
    # Valores típicos de kg m-2 s-1 (<< 5) para disparar conversão ×86400.
    valores = rng.uniform(0.0, 2.5e-4, size=(len(tempo), ny, nx)).astype(np.float32)

    ds = xr.Dataset(
        data_vars={
            "pr": (("time", "lat", "lon"), valores, {"units": "kg m-2 s-1"}),
        },
        coords={
            "time": tempo,
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
        attrs={"experiment_id": "rcp45"},
    )
    destino.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(destino, engine="netcdf4")


def _gerar_cftime(destino: Path) -> None:
    """Grade 5x5, 3 anos, calendário 360_day, lat/lon 2D, ``pr`` em mm/day."""
    rng = np.random.default_rng(seed=7)
    ny, nx = 5, 5
    lat_1d = np.linspace(-10.0, -5.0, ny, dtype=np.float32)
    lon_1d = np.linspace(-60.0, -55.0, nx, dtype=np.float32)
    lon_2d, lat_2d = np.meshgrid(lon_1d, lat_1d)
    tempo = xr.date_range(
        "2026-01-01", periods=360 * 3, freq="D", calendar="360_day", use_cftime=True
    )
    valores = rng.uniform(0.0, 40.0, size=(len(tempo), ny, nx)).astype(np.float32)

    ds = xr.Dataset(
        data_vars={
            "pr": (("time", "y", "x"), valores, {"units": "mm/day"}),
        },
        coords={
            "time": tempo,
            "lat": (("y", "x"), lat_2d, {"units": "degrees_north"}),
            "lon": (("y", "x"), lon_2d, {"units": "degrees_east"}),
        },
        attrs={"experiment_id": "rcp85"},
    )
    destino.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(destino, engine="netcdf4")


def _gerar_csv_pontos(destino: Path) -> None:
    """CSV fixo de 5 pontos dentro da grade básica."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    linhas = [
        ["lat", "lon", "cidade", "estado"],
        [-22.9, -46.5, "PontoA", "SP"],
        [-22.0, -45.5, "PontoB", "SP"],
        [-21.5, -44.8, "PontoC", "MG"],
        [-20.8, -44.2, "PontoD", "MG"],
        [-23.0, -47.0, "PontoE", "SP"],
    ]
    with destino.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(linhas)


def _rodar_legacy_grade(nc: Path, out_csv: str, p95_baseline: str, bbox: list[str]) -> None:
    cmd = [
        sys.executable,
        str(LEGACY / "cordex_pr_freq_intensity.py"),
        "--glob",
        str(nc),
        "--bbox",
        *bbox,
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
        p95_baseline,
    ]
    resultado = subprocess.run(cmd, capture_output=True, text=True, cwd=str(RAIZ), check=False)
    if resultado.returncode != 0:
        print("STDOUT:", resultado.stdout)
        print("STDERR:", resultado.stderr)
        raise RuntimeError(
            f"Falha ao rodar cordex_pr_freq_intensity contra {nc.name} (rc={resultado.returncode})"
        )


def _rodar_legacy_pontos(nc: Path, points_csv: Path, out_csv: str, p95_baseline: str) -> None:
    cmd = [
        sys.executable,
        str(LEGACY / "gera_pontos_fornecedores.py"),
        "--glob",
        str(nc),
        "--points-csv",
        str(points_csv),
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
        p95_baseline,
    ]
    resultado = subprocess.run(cmd, capture_output=True, text=True, cwd=str(RAIZ), check=False)
    if resultado.returncode != 0:
        print("STDOUT:", resultado.stdout)
        print("STDERR:", resultado.stderr)
        raise RuntimeError(
            f"Falha ao rodar gera_pontos_fornecedores contra {nc.name} (rc={resultado.returncode})"
        )


def _contar_linhas(caminho: Path) -> int:
    with caminho.open("r", encoding="utf-8-sig") as f:
        return sum(1 for _ in f) - 1  # desconta header


def main() -> int:
    DIR_NETCDF.mkdir(parents=True, exist_ok=True)
    DIR_BASELINES.mkdir(parents=True, exist_ok=True)

    # 1. Gerar .nc sintéticos.
    nc_basico = DIR_NETCDF / "cordex_sintetico_basico.nc"
    nc_cftime = DIR_NETCDF / "cordex_sintetico_cftime.nc"
    print(f"Gerando {nc_basico.name}...")
    _gerar_basico(nc_basico)
    print(f"Gerando {nc_cftime.name}...")
    _gerar_cftime(nc_cftime)

    # 2. Gerar CSV de pontos fixo.
    pontos_csv = DIR_NETCDF / "pontos_fixos.csv"
    _gerar_csv_pontos(pontos_csv)

    # 3. Rodar legados.
    baseline_grade_basico = DIR_BASELINES / "baseline_grade_basico.csv"
    baseline_grade_cftime = DIR_BASELINES / "baseline_grade_cftime.csv"
    baseline_pontos_basico = DIR_BASELINES / "baseline_pontos_basico.csv"
    baseline_pontos_cftime = DIR_BASELINES / "baseline_pontos_cftime.csv"

    print("Rodando legacy/cordex_pr_freq_intensity.py (básico)...")
    _rodar_legacy_grade(
        nc_basico,
        str(baseline_grade_basico),
        p95_baseline="2026-2030",
        bbox=["-23.0", "-20.0", "-47.0", "-44.0"],
    )

    print("Rodando legacy/cordex_pr_freq_intensity.py (cftime)...")
    # O script legado cai em pd.to_datetime(cftime.Datetime360Day(...)) e não suporta
    # calendários 360_day. Capturamos o comportamento atual como um CSV "vazio" (apenas
    # header) para preservar a paridade bit-a-bit: o novo código deve reproduzir isto
    # no Slice 3 OU documentar formalmente a correção em ADR.
    _rodar_legacy_grade(
        nc_cftime,
        str(baseline_grade_cftime),
        p95_baseline="2026-2028",
        bbox=["-10.0", "-5.0", "-60.0", "-55.0"],
    )
    if not baseline_grade_cftime.exists() or _contar_linhas(baseline_grade_cftime) < 0:
        print(
            "AVISO: legacy/cordex_pr_freq_intensity.py não gerou saída para "
            "calendário 360_day (limitação conhecida do código antigo). "
            "Gravando CSV header-only como baseline."
        )
        header = (
            "year,lat,lon,scenario,variable,wet_days,sdii,rx1day,rx5day,"
            "r20mm,r50mm,r95ptot_mm,r95ptot_frac,uf,municipio\n"
        )
        baseline_grade_cftime.write_text(header, encoding="utf-8")

    print("Rodando legacy/gera_pontos_fornecedores.py (básico)...")
    _rodar_legacy_pontos(
        nc_basico,
        pontos_csv,
        str(baseline_pontos_basico),
        p95_baseline="2026-2030",
    )

    print("Rodando legacy/gera_pontos_fornecedores.py (cftime)...")
    _rodar_legacy_pontos(
        nc_cftime,
        pontos_csv,
        str(baseline_pontos_cftime),
        p95_baseline="2026-2028",
    )

    # 4. Resumo.
    print("\n=== Resumo da baseline sintética ===")
    for caminho in (
        baseline_grade_basico,
        baseline_grade_cftime,
        baseline_pontos_basico,
        baseline_pontos_cftime,
    ):
        print(f"  {caminho.name}: {_contar_linhas(caminho)} linhas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
