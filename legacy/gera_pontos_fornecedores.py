#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import shutil
import tempfile
from glob import glob
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr

# ===================== Logs =====================

def log(msg: str) -> None:
    print(f"[LOG] {msg}")

# ===================== Helpers =====================

_SCENARIO_RE = re.compile(r"(rcp\d{2}|ssp\d{3})", re.IGNORECASE)

def infer_scenario(path: str, ds: Optional[xr.Dataset] = None) -> str:
    """Prioriza rcp/ssp no nome; senão tenta atributos."""
    name = os.path.basename(path)
    m = _SCENARIO_RE.search(name)
    if m:
        return m.group(1).lower()
    if ds is not None:
        for k in ("experiment_id", "scenario", "experiment"):
            v = str(ds.attrs.get(k, "")).strip().lower()
            if v:
                return v
    return "unknown"

def open_nc_multi(path: str) -> xr.Dataset:
    """Tenta engines diferentes para abrir o NetCDF (forçando cftime quando possível)."""
    last_err: Optional[Exception] = None
    for eng in ["netcdf4", "h5netcdf", "scipy", None]:
        for use_cftime in (True, False):
            try:
                ds = xr.open_dataset(
                    path, engine=eng, decode_times=True, mask_and_scale=True,
                    **({"use_cftime": True} if use_cftime else {})
                )
                _ = list(ds.dims)  # força leitura do header
                log(f"Abrido com engine={eng or 'auto'} (use_cftime={use_cftime}) -> {os.path.basename(path)}")
                return ds
            except TypeError as e:
                # versões antigas não aceitam use_cftime -> tenta sem
                if "unexpected keyword argument 'use_cftime'" in str(e):
                    continue
                last_err = e
            except Exception as e:
                last_err = e
                # log intermediário deixa o rastro dos engines testados
                log(f"Falha engine={eng}, use_cftime={use_cftime}: {type(e).__name__}: {e}")
    raise last_err  # type: ignore[misc]

def safe_open_with_copy(path: str, force_copy: bool):
    """Se houver lock/erro de permissão, copia para /tmp e abre de lá."""
    if force_copy:
        tmpdir = tempfile.mkdtemp(prefix="nc_copy_")
        dst = os.path.join(tmpdir, os.path.basename(path))
        shutil.copy2(path, dst)
        ds = open_nc_multi(dst)
        return ds, tmpdir, dst
    try:
        ds = open_nc_multi(path)
        return ds, None, path
    except PermissionError:
        log(f"Permissão negada: {os.path.basename(path)}. Copiando para pasta temporária…")
        tmpdir = tempfile.mkdtemp(prefix="nc_copy_")
        dst = os.path.join(tmpdir, os.path.basename(path))
        shutil.copy2(path, dst)
        ds = open_nc_multi(dst)
        return ds, tmpdir, dst

def guess_latlon_vars(ds: xr.Dataset) -> Tuple[str, str]:
    """Encontra nomes das variáveis/coords lat/lon (1D ou 2D)."""
    for la in ("lat", "latitude", "y"):
        for lo in ("lon", "longitude", "x"):
            if la in ds.coords and lo in ds.coords:
                return la, lo
            if la in ds.variables and lo in ds.variables:
                return la, lo
    raise ValueError("Não encontrei variáveis/coords de latitude/longitude.")

def coords_to_2d(lat_vals: np.ndarray, lon_vals: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Retorna (lat2d, lon2d) no shape da grade (2D)."""
    lat_vals = np.asarray(lat_vals)
    lon_vals = np.asarray(lon_vals)
    if lat_vals.ndim == 2 and lon_vals.ndim == 2:
        return lat_vals, lon_vals
    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon_vals, lat_vals)  # (ny, nx)
        return lat2d, lon2d
    # fallback
    lon2d, lat2d = np.meshgrid(np.asarray(lon_vals).reshape(-1), np.asarray(lat_vals).reshape(-1))
    return lat2d, lon2d

def ensure_lon_negpos180(lon_vals: np.ndarray) -> np.ndarray:
    """Converte 0–360 → -180…180."""
    return ((lon_vals + 180.0) % 360.0) - 180.0

def convert_pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    """
    Converte precipitação diária:
    - Se unidades forem kg m-2 s-1 / mm/s → mm/dia (×86400).
    - Se já estiver em mm/day, mantém.
    """
    units = (da.attrs.get("units", "") or "").lower()
    vmax = float(da.max())
    if ("kg m-2 s-1" in units) or ("kg m^-2 s^-1" in units) or ("mm s-1" in units) or ("mm/s" in units) or vmax < 5.0:
        da = da * 86400.0
    da.attrs["units"] = "mm/day"
    return da

def parse_baseline(years_str: Optional[str]) -> Optional[Tuple[int, int]]:
    """'1995-2014' → (1995,2014)"""
    if not years_str:
        return None
    s = years_str.strip()
    a, b = s.split("-", 1)
    return int(a), int(b)

# ===================== Índices =====================

def annual_indices_for_series(series: np.ndarray,
                              wet_thr: float,
                              p95_thr: Optional[float],
                              r_heavy: Tuple[float, float]) -> Tuple[int, float, float, float, int, int, float, float]:
    """
    Calcula índices para um vetor 1D (dias do ano) em mm/dia.
    Retorna: wet_days, sdii, rx1day, rx5day, r20mm, r50mm, r95ptot_mm, r95ptot_frac

    - Frequência (wet_days): nº de dias com pr >= wet_thr (T, default 20 mm)
    - Intensidade (sdii): média de pr apenas desses dias (pr >= wet_thr)
    """
    x = np.asarray(series, dtype=np.float32)
    valid = np.isfinite(x)
    if not np.any(valid):
        return 0, np.nan, np.nan, np.nan, 0, 0, np.nan, np.nan
    x = x.copy()
    x[~valid] = 0.0

    wet_mask = x >= wet_thr
    wet_days = int(wet_mask.sum())
    sdii = float(x[wet_mask].mean()) if wet_days > 0 else np.nan

    rx1day = float(x.max()) if x.size > 0 else np.nan

    if x.size >= 5:
        k = np.ones(5, dtype=np.float32)
        acc5 = np.convolve(x, k, mode="valid")
        rx5day = float(acc5.max())
    else:
        rx5day = np.nan

    r20mm = int((x >= r_heavy[0]).sum())
    r50mm = int((x >= r_heavy[1]).sum())

    if p95_thr is not None:
        heavy = x > p95_thr
        r95ptot_mm = float(x[heavy].sum())
        tot = float(x.sum())
        r95ptot_frac = (r95ptot_mm / tot) if tot > 0 else np.nan
    else:
        r95ptot_mm = np.nan
        r95ptot_frac = np.nan

    return wet_days, sdii, rx1day, rx5day, r20mm, r50mm, r95ptot_mm, r95ptot_frac


def compute_p95_grid(pr_da, baseline, p95_wet_thr):
    """
    Calcula o P95 por pixel, sem estourar a RAM.
    Retorna um array 2D (lat, lon).
    """
    if baseline is None:
        return None

    # extrair anos via cftime-compatible
    years = pr_da["time"].dt.year

    # filtrar intervalo
    mask = (years >= baseline[0]) & (years <= baseline[1])
    da_base = pr_da.sel(time=mask)

    if da_base.sizes.get("time", 0) == 0:
        log(f"⚠ Nenhum dado no baseline {baseline}")
        return None

    # considerar apenas dias "úmidos"
    da_wet = da_base.where(da_base >= p95_wet_thr)

    # QUANTILE pixel-a-pixel (não explode memória)
    thr = da_wet.quantile(0.95, dim="time", skipna=True)

    # retorna como numpy 2D
    return thr.values.astype("float32")

# ===================== Core: Pontos exatos =====================

def normalize_lon(lon: float) -> float:
    """Garante lon em [-180,180]."""
    return float(((float(lon) + 180.0) % 360.0) - 180.0)

def find_nearest_idx(lat2d: np.ndarray, lon2d: np.ndarray, lat: float, lon: float) -> Tuple[int, int]:
    """Retorna (iy, ix) do grid mais próximo à coordenada (lat, lon)."""
    # Flatten
    latf = lat2d.reshape(-1)
    lonf = ensure_lon_negpos180(lon2d.reshape(-1))
    target_lon = normalize_lon(lon)
    target_lat = float(lat)

    # Distância euclidiana em graus (ok para ~25 km de resolução)
    d = (latf - target_lat) ** 2 + (lonf - target_lon) ** 2
    k = int(np.argmin(d))
    ny, nx = lat2d.shape
    iy, ix = divmod(k, nx)
    return iy, ix

def load_points_csv(points_csv: str) -> pd.DataFrame:
    """
    Lê CSV de pontos. Aceita vírgula como separador decimal.
    Campos esperados:
      - lat, lon (obrigatórios)
      - cidade, estado (opcional; pode vir como City/Estado, Municipio/UF)
    """
    # Muitos CSVs exportados do Excel em PT-BR vêm em cp1252/latin1 e, às vezes, com ';'.
    # Tenta combinações comuns antes de falhar.
    read_attempts = []
    df = None
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        for sep in (",", ";"):
            try:
                df = pd.read_csv(points_csv, dtype=str, encoding=enc, sep=sep)
                # Se o separador estiver errado, geralmente tudo vira 1 coluna só.
                if len(df.columns) <= 1:
                    continue
                log(f"CSV de pontos lido com encoding={enc}, sep='{sep}'")
                break
            except Exception as e:
                read_attempts.append(f"encoding={enc}, sep='{sep}' -> {type(e).__name__}: {e}")
        if df is not None and len(df.columns) > 1:
            break

    if df is None or len(df.columns) <= 1:
        details = "\n".join(read_attempts[-4:]) if read_attempts else "sem detalhes"
        raise ValueError(
            "Nao foi possivel ler o CSV de pontos com os formatos testados. "
            "Verifique encoding/separador do arquivo. Ultimas tentativas:\n"
            f"{details}"
        )

    cols = {c.lower(): c for c in df.columns}

    # mapear possíveis nomes
    lat_col = next((cols[c] for c in cols if c in ("lat","latitude")), None)
    lon_col = next((cols[c] for c in cols if c in ("lon","longitude","long")), None)
    city_col = next((cols[c] for c in cols if c in ("cidade","municipio","city")), None)
    uf_col   = next((cols[c] for c in cols if c in ("estado","uf","state")), None)

    if not lat_col or not lon_col:
        raise ValueError("CSV de pontos deve ter colunas 'lat' e 'lon' (ou 'latitude'/'longitude').")

    def parse_num(x):
        if pd.isna(x): return np.nan
        s = str(x).strip().replace(" ", "")
        # aceita vírgula decimal
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return np.nan

    out = pd.DataFrame({
        "lat_in": df[lat_col].map(parse_num),
        "lon_in": df[lon_col].map(parse_num),
    })
    out["cidade"] = df[city_col] if city_col else ""
    out["estado"] = df[uf_col]   if uf_col   else ""

    # drop linhas sem coordenada válida
    out = out[np.isfinite(out["lat_in"]) & np.isfinite(out["lon_in"])].reset_index(drop=True)
    if out.empty:
        raise ValueError("Nenhum ponto válido no CSV.")

    return out

def process_files_for_points(nc_files: List[str],
                             points: pd.DataFrame,
                             out_csv: str,
                             wet_thr: float,
                             p95_wet_thr: float,
                             heavy20: float,
                             heavy50: float,
                             baseline: Optional[Tuple[int, int]],
                             copy_all: bool) -> None:
    """
    Para cada arquivo .nc, calcula índices apenas no grid mais próximo de cada ponto informado.
    Escreve no CSV uma linha por (ponto, ano).
    """
    header = [
        "scenario","file","cidade","estado",
        "lat_input","lon_input","lat_grid","lon_grid",
        "year",
        "wet_days","sdii","rx1day","rx5day","r20mm","r50mm","r95ptot_mm","r95ptot_frac"
    ]
    wrote_header = [False]

    if os.path.exists(out_csv):
        os.remove(out_csv)

    for path in nc_files:
        ds, tmpdir, _ = safe_open_with_copy(path, force_copy=copy_all)
        try:
            scenario = infer_scenario(path, ds)

            if "pr" not in ds.data_vars:
                log(f"⚠️ Arquivo sem variável 'pr': {os.path.basename(path)} — pulando.")
                continue
            da = ds["pr"]
            if "time" not in da.dims:
                log(f"⚠️ 'pr' sem dimensão tempo em {os.path.basename(path)} — pulando.")
                continue

            da = convert_pr_to_mm_per_day(da)

            lat_name, lon_name = guess_latlon_vars(ds)
            lat_vals = np.asarray(ds[lat_name].values)
            lon_vals = np.asarray(ds[lon_name].values)
            lat2d, lon2d = coords_to_2d(lat_vals, lon_vals)

            # P95 baseline por célula
            p95_grid = compute_p95_grid(da, baseline=baseline, p95_wet_thr=p95_wet_thr)
            p95_flat = p95_grid.reshape(-1) if p95_grid is not None else None

            # time → anos e índices por ano

            time_years = da["time"].dt.year.values
            years_sorted = sorted(np.unique(time_years))


            # Pré-flatten para indexar rápido
            ny, nx = lat2d.shape
            lat_flat = lat2d.reshape(-1).astype("float64")
            lon_flat = ensure_lon_negpos180(lon2d.reshape(-1)).astype("float64")

            # abre writer
            mode = "a" if wrote_header[0] else "w"
            with open(out_csv, mode, newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                if not wrote_header[0]:
                    w.writerow(header)
                    wrote_header[0] = True

                # Para cada ponto: encontra (iy,ix) e calcula índices ANO A ANO
                for _, row in points.iterrows():
                    iy, ix = find_nearest_idx(lat2d, lon2d, row["lat_in"], row["lon_in"])
                    grid_lat = float(lat2d[iy, ix])
                    grid_lon = float(ensure_lon_negpos180(lon2d[iy, ix]))

                    # extrai série diária do pixel
                    # da.dims deve ser (time, y, x) ou (time, lat, lon)
                    pix = da.isel({da.dims[-2]: iy, da.dims[-1]: ix}).values  # (time,)
                    if pix.ndim != 1:
                        # fallback: garante vetor 1D
                        pix = np.asarray(pix).reshape(-1)

                    # separa por ano
                    for yr in years_sorted:
                        mask = (time_years == yr)
                        if not mask.any():
                            continue
                        series = pix[mask]

                        # p95 do pixel (se existir)
                        if p95_flat is not None:
                            k = iy * nx + ix
                            p95_thr = float(p95_flat[k]) if np.isfinite(p95_flat[k]) else None
                        else:
                            p95_thr = None

                        wet_days, sdii, rx1, rx5, r20, r50, r95mm, r95frac = annual_indices_for_series(
                            series, wet_thr=wet_thr, p95_thr=p95_thr, r_heavy=(heavy20, heavy50)
                        )

                        w.writerow([
                            scenario, os.path.basename(path),
                            row.get("cidade",""), row.get("estado",""),
                            float(row["lat_in"]), float(row["lon_in"]),
                            grid_lat, grid_lon,
                            int(yr),
                            wet_days, sdii, rx1, rx5, r20, r50, r95mm, r95frac
                        ])

            log(f"✔ {os.path.basename(path)} — pontos processados: {len(points)}")

        finally:
            try:
                ds.close()
            except Exception:
                pass
            if tmpdir and os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)

# ===================== Main =====================

def main():
    ap = argparse.ArgumentParser(description="Análise de pontos exatos em arquivos CORDEX (precipitação diária).")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-dir", help="Pasta (recursivo) com .nc.")
    src.add_argument("--glob", help="Padrão glob, ex.: C:\\dados\\*.nc")

    ap.add_argument("--points-csv", required=True,
                    help="CSV com colunas lat, lon (aceita vírgula decimal). Opcional: cidade, estado.")
    ap.add_argument("--out", required=True, help="CSV de saída com índices por (ponto, ano).")
    ap.add_argument("--copy-all", action="store_true", help="Copiar cada .nc para /tmp antes de abrir.")

    # Parâmetros dos índices
    ap.add_argument("--freq-thr-mm", type=float, default=20.0,
                    help="Limiar T (mm/dia) para frequência/intensidade (dias com pr >= T). Default 20.")
    ap.add_argument("--p95-wet-thr", type=float, default=1.0,
                    help="Limiar de 'dia chuvoso' (mm/dia) p/ P95 do baseline (ETCCDI). Default 1.0.")
    ap.add_argument("--heavy20", type=float, default=20.0, help="Limiar p/ r20mm (mm). Default 20")
    ap.add_argument("--heavy50", type=float, default=50.0, help="Limiar p/ r50mm (mm). Default 50")
    ap.add_argument("--p95-baseline", type=str, default="2026-2035",
                    help="Baseline para P95 (ex.: '1995-2014' ou '2026-2035'). Use None p/ desativar.")

    args = ap.parse_args()

    baseline = None if (args.p95_baseline is None or str(args.p95_baseline).lower()=="none") else parse_baseline(args.p95_baseline)

    # lista de arquivos
    if args.input_dir:
        files = []
        for root, _, _ in os.walk(args.input_dir):
            files.extend(glob(os.path.join(root, "*.nc")))
    else:
        files = glob(args.glob)

    if not files:
        raise FileNotFoundError("Nenhum .nc encontrado.")
    files = sorted(files)
    log(f"Arquivos encontrados: {len(files)}")
    for f in files:
        log(f"  • {f}")

    # carregar pontos
    points = load_points_csv(args.points_csv)
    log(f"Pontos válidos: {len(points)}")

    # rodar
    process_files_for_points(
        nc_files=files,
        points=points,
        out_csv=args.out,
        wet_thr=float(args.freq_thr_mm),
        p95_wet_thr=float(args.p95_wet_thr),
        heavy20=float(args.heavy20),
        heavy50=float(args.heavy50),
        baseline=baseline,
        copy_all=args.copy_all
    )

    log(f"✅ Concluído. CSV final: {os.path.abspath(args.out)}")

if __name__ == "__main__":
    main()