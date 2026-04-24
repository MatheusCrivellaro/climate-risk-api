"""Gera arquivos NetCDF sintéticos para testes do leitor multi-variável (Slice 13).

Gera três ``.nc`` em ``tests/fixtures/climatologia_multi/``:

- ``pr_sintetico.nc``: grade 3x3, 10 dias, kg m-2 s-1, calendário padrão.
- ``tas_sintetico.nc``: grade 3x3, 10 dias, Kelvin, calendário padrão
  (mistura de dias quentes e frios por construção).
- ``evspsbl_sintetico.nc``: grade **2x2 diferente** (para simular grades
  desalinhadas), 10 dias, kg m-2 s-1, calendário ``noleap``.

Todos reportam ``experiment_id = rcp45`` e período base 2026-01-01 a
2026-01-10 (gregoriano) / 360 correspondentes do ``noleap`` (janeiro não é
afetado pela diferença de calendários, então os timestamps batem 1:1).

Além dos ``.nc``, grava ``esperado.json`` em
``tests/fixtures/baselines/estresse_hidrico_sintetico/`` com os valores
calculados à mão da série central — serve de golden baseline para
``tests/unit/domain/test_estresse_hidrico.py::test_regressao_baseline``.

Executar com ``uv run python tests/fixtures/climatologia_multi/gerar_fixtures.py``;
idempotente — sobrescreve se já existir.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

RAIZ = Path(__file__).resolve().parents[3]
DIR_FIXTURES = RAIZ / "tests" / "fixtures" / "climatologia_multi"
DIR_BASELINE = RAIZ / "tests" / "fixtures" / "baselines" / "estresse_hidrico_sintetico"


def _valores_pr_kg_m2_s() -> np.ndarray:
    """Valores conhecidos de precipitação para os 10 dias, ponto central (1, 1).

    Em ``mm/dia`` (equivalente ao kg m-2 s-1 x 86400), a série do ponto
    central é: ``[0.5, 0.2, 5.0, 0.0, 0.1, 2.0, 0.3, 0.0, 10.0, 0.4]``.
    O ponto central usa o índice ``(dia, 1, 1)`` em grade 3x3. Os demais
    pontos recebem valores sintéticos derivados, suficientes para testes
    de leitura mas sem importância para o teste de regressão numérica.
    """
    pr_central_mm_dia = np.array(
        [0.5, 0.2, 5.0, 0.0, 0.1, 2.0, 0.3, 0.0, 10.0, 0.4], dtype=np.float32
    )
    ny, nx = 3, 3
    valores_mm_dia = np.zeros((10, ny, nx), dtype=np.float32)
    for dia in range(10):
        valores_mm_dia[dia, :, :] = pr_central_mm_dia[dia] * np.array(
            [[0.8, 1.0, 1.2], [0.9, 1.0, 1.1], [0.7, 1.0, 1.3]], dtype=np.float32
        )
    # Converter para kg m-2 s-1 (/86400).
    return valores_mm_dia / 86400.0


def _valores_tas_kelvin() -> np.ndarray:
    """Temperatura em K, dia-a-dia do ponto central.

    Série em °C: ``[28.0, 32.0, 31.5, 25.0, 33.0, 29.0, 35.0, 27.0, 30.5, 24.0]``.
    Em Kelvin: soma-se 273.15. Dias com ``tas >= 30`` (limiar default): 1, 2,
    4, 6, 8 → índices 1, 2, 4, 6, 8 da série.
    """
    tas_central_c = np.array(
        [28.0, 32.0, 31.5, 25.0, 33.0, 29.0, 35.0, 27.0, 30.5, 24.0], dtype=np.float32
    )
    ny, nx = 3, 3
    valores_c = np.zeros((10, ny, nx), dtype=np.float32)
    for dia in range(10):
        valores_c[dia, :, :] = tas_central_c[dia] + np.array(
            [[-1.0, 0.0, 1.0], [-0.5, 0.0, 0.5], [-1.5, 0.0, 1.5]], dtype=np.float32
        )
    return valores_c + 273.15


def _valores_evap_kg_m2_s() -> np.ndarray:
    """Evaporação em kg m-2 s-1, grade 2x2 diferente (desalinhada).

    Série em mm/dia do ponto (0, 0):
    ``[3.0, 4.5, 2.0, 1.0, 5.0, 2.5, 6.0, 1.5, 4.0, 0.8]``.
    """
    evap_central_mm_dia = np.array(
        [3.0, 4.5, 2.0, 1.0, 5.0, 2.5, 6.0, 1.5, 4.0, 0.8], dtype=np.float32
    )
    ny, nx = 2, 2
    valores_mm_dia = np.zeros((10, ny, nx), dtype=np.float32)
    for dia in range(10):
        valores_mm_dia[dia, :, :] = evap_central_mm_dia[dia] * np.array(
            [[1.0, 1.1], [0.9, 1.0]], dtype=np.float32
        )
    return valores_mm_dia / 86400.0


def gerar_pr(destino: Path) -> None:
    """Grade 3x3, 10 dias, calendário gregoriano, kg m-2 s-1."""
    lat = np.array([-20.0, -21.0, -22.0], dtype=np.float32)
    lon = np.array([-46.0, -45.0, -44.0], dtype=np.float32)
    tempo = pd.date_range("2026-01-01", periods=10, freq="D")
    valores = _valores_pr_kg_m2_s()
    ds = xr.Dataset(
        data_vars={
            "pr": (("time", "lat", "lon"), valores, {"units": "kg m-2 s-1"}),
        },
        coords={
            "time": tempo,
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
        attrs={"experiment_id": "rcp45", "institution": "TEST"},
    )
    destino.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(destino, engine="netcdf4")


def gerar_tas(destino: Path) -> None:
    """Mesma grade 3x3, 10 dias, Kelvin."""
    lat = np.array([-20.0, -21.0, -22.0], dtype=np.float32)
    lon = np.array([-46.0, -45.0, -44.0], dtype=np.float32)
    tempo = pd.date_range("2026-01-01", periods=10, freq="D")
    valores = _valores_tas_kelvin()
    ds = xr.Dataset(
        data_vars={
            "tas": (("time", "lat", "lon"), valores, {"units": "K"}),
        },
        coords={
            "time": tempo,
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
        attrs={"experiment_id": "rcp45", "institution": "TEST"},
    )
    destino.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(destino, engine="netcdf4")


def gerar_evap(destino: Path) -> None:
    """Grade 2x2 diferente (desalinhada), 10 dias, calendário ``noleap``."""
    lat = np.array([-20.5, -21.5], dtype=np.float32)
    lon = np.array([-45.5, -44.5], dtype=np.float32)
    tempo = xr.date_range("2026-01-01", periods=10, freq="D", calendar="noleap", use_cftime=True)
    valores = _valores_evap_kg_m2_s()
    ds = xr.Dataset(
        data_vars={
            "evspsbl": (("time", "lat", "lon"), valores, {"units": "kg m-2 s-1"}),
        },
        coords={
            "time": tempo,
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
        attrs={"experiment_id": "rcp45", "institution": "TEST"},
    )
    destino.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(destino, engine="netcdf4")


def calcular_baseline_esperada() -> dict[str, float | int]:
    """Valores calculados à mão para a série do ponto central (10 dias).

    Série pr (mm/dia):   [0.5, 0.2, 5.0, 0.0, 0.1, 2.0, 0.3, 0.0, 10.0, 0.4]
    Série tas (°C):      [28.0, 32.0, 31.5, 25.0, 33.0, 29.0, 35.0, 27.0, 30.5, 24.0]
    Série evap (mm/dia): [3.0, 4.5, 2.0, 1.0, 5.0, 2.5, 6.0, 1.5, 4.0, 0.8]

    Limiares (default):  pr<=1.0, tas>=30.0

    Dia seco (pr<=1.0): índices [0, 1, 3, 4, 6, 7, 9]  → pr = 0.5, 0.2, 0.0, 0.1, 0.3, 0.0, 0.4
    Dia quente (tas>=30.0): índices [1, 2, 4, 6, 8]     → tas = 32.0, 31.5, 33.0, 35.0, 30.5
    Dia seco E quente (AND): interseção [1, 4, 6]       → 3 dias

    Déficit diário (evap - pr) nos 10 dias:
        [3.0-0.5,  4.5-0.2, 2.0-5.0, 1.0-0.0,  5.0-0.1,
         2.5-2.0,  6.0-0.3, 1.5-0.0, 4.0-10.0, 0.8-0.4]
      = [2.5,      4.3,    -3.0,     1.0,     4.9,
         0.5,      5.7,     1.5,    -6.0,      0.4]

    Intensidade (soma do déficit nos índices 1, 4, 6): 4.3 + 4.9 + 5.7 = 14.9
    Déficit total (soma dos 10): 2.5 + 4.3 - 3.0 + 1.0 + 4.9 + 0.5 + 5.7 + 1.5 - 6.0 + 0.4 = 11.8
    """
    return {
        "dias_secos_quentes": 3,
        "intensidade_estresse": 14.9,
        "deficit_total_mm": 11.8,
    }


def main() -> int:
    DIR_FIXTURES.mkdir(parents=True, exist_ok=True)
    DIR_BASELINE.mkdir(parents=True, exist_ok=True)

    arquivos = {
        "pr_sintetico.nc": gerar_pr,
        "tas_sintetico.nc": gerar_tas,
        "evspsbl_sintetico.nc": gerar_evap,
    }
    for nome, fn in arquivos.items():
        destino = DIR_FIXTURES / nome
        print(f"Gerando {destino.relative_to(RAIZ)}...")
        fn(destino)

    baseline = DIR_BASELINE / "esperado.json"
    print(f"Gravando {baseline.relative_to(RAIZ)}...")
    baseline.write_text(json.dumps(calcular_baseline_esperada(), indent=2) + "\n")

    print("Pronto.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
