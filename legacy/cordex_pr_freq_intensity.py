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

# ---- geocodificação opcional (UF/Município)
try:
    import geopandas as gpd
    from shapely.geometry import Point
    GEOS_OK = True
except Exception:
    GEOS_OK = False


# ===================== Utils =====================

def log(msg: str) -> None:
    print(f"[LOG] {msg}")

_SCENARIO_RE = re.compile(r"(rcp\d{2}|ssp\d{3})", re.IGNORECASE)

def infer_scenario(path: str, ds: Optional[xr.Dataset] = None) -> str:
    """Prioriza rcp/ssp no nome; senão tenta atributos (experiment_id/scenario)."""
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
    """Tenta engines diferentes para abrir o NetCDF."""
    last_err: Optional[Exception] = None
    for eng in ["netcdf4", "h5netcdf", "scipy", None]:
        try:
            ds = xr.open_dataset(path, engine=eng, decode_times=True, mask_and_scale=True)
            _ = list(ds.dims)  # força leitura do header
            log(f"Abrido com engine={eng or 'auto'} -> {os.path.basename(path)}")
            return ds
        except Exception as e:
            last_err = e
            log(f"Falha engine={eng}: {type(e).__name__}: {e}")
    raise last_err  # type: ignore[misc]

def safe_open_with_copy(path: str, force_copy: bool):
    """Se o arquivo estiver bloqueado (PermissionError) ou force_copy=True, copia para /tmp e abre de lá."""
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

def parse_years_range(years_str: Optional[str]) -> Optional[Tuple[int, int]]:
    """'1995-2014' → (1995, 2014)."""
    if not years_str:
        return None
    s = years_str.strip()
    if "-" not in s:
        raise ValueError("Formato do período deve ser 'YYYY-YYYY'.")
    a, b = s.split("-", 1)
    return int(a), int(b)

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
    """
    Retorna (lat2d, lon2d) no mesmo shape da grade:
    - Se já forem 2D, retorna como estão;
    - Se forem 1D, faz meshgrid (lon, lat) -> (ny, nx) e devolve (lat2d, lon2d).
    """
    lat_vals = np.asarray(lat_vals)
    lon_vals = np.asarray(lon_vals)
    if lat_vals.ndim == 2 and lon_vals.ndim == 2:
        return lat_vals, lon_vals
    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon_vals, lat_vals)  # (ny, nx)
        return lat2d, lon2d
    # fallback: força 2D
    lon2d, lat2d = np.meshgrid(np.asarray(lon_vals).reshape(-1), np.asarray(lat_vals).reshape(-1))
    return lat2d, lon2d

def ensure_lon_negpos180(lon_vals: np.ndarray) -> np.ndarray:
    """Converte 0–360 → -180…180; valores já negativos permanecem."""
    return ((lon_vals + 180.0) % 360.0) - 180.0

def build_in_bbox_mask(lat2d: np.ndarray, lon2d: np.ndarray, bbox) -> np.ndarray:
    """
    Gera máscara 1D (após flatten) para pontos dentro do BBOX.
    bbox = (lat_min, lat_max, lon_min, lon_max), com lon em [-180,180].
    """
    lat_flat = lat2d.reshape(-1)
    lon_flat = ensure_lon_negpos180(lon2d.reshape(-1))
    lat_min, lat_max, lon_min, lon_max = bbox
    return (
        (lat_flat >= lat_min) & (lat_flat <= lat_max) &
        (lon_flat >= lon_min) & (lon_flat <= lon_max)
    )

def key_latlon(lat: float, lon: float, nd=5) -> Tuple[float, float]:
    return (round(float(lat), nd), round(float(lon), nd))

def convert_pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    """
    Converte precipitação diária:
    - Se unidades forem kg m-2 s-1 (ou similares) → mm/dia (×86400).
    - Se já estiver em mm/day, mantém.
    """
    units = (da.attrs.get("units", "") or "").lower()
    vmax = float(da.max())
    if ("kg m-2 s-1" in units) or ("kg m^-2 s^-1" in units) or ("mm s-1" in units) or ("mm/s" in units) or vmax < 5.0:
        da = da * 86400.0
    da.attrs["units"] = "mm/day"
    return da


# ===================== Geocodificação (UF/Município) =====================

def pick_name_col(gdf: "gpd.GeoDataFrame", prefer_uf=True) -> str:
    cols_lower = {c.lower(): c for c in gdf.columns}
    if prefer_uf:
        for c in ("sigla_uf","uf","sigla","nm_uf","nome","name"):
            if c in cols_lower: return cols_lower[c]
    else:
        for c in ("nm_mun","nome_mun","nm_municip","nm_municipio","name"):
            if c in cols_lower: return cols_lower[c]
    return list(gdf.columns)[0]

def geocode_points_with_shapes(points_df: pd.DataFrame,
                               shp_uf: Optional[str],
                               shp_mun: Optional[str]) -> Dict[Tuple[float, float], Tuple[str, str]]:
    """
    (lat,lon) -> (UF, Município) via sjoin 'within'. Reindexa p/ 1:1 com os pontos.
    """
    if not GEOS_OK:
        log("⚠️ Geopandas/Shapely não disponíveis — instale: pip install geopandas shapely rtree")
        return {}

    if not (shp_uf and os.path.exists(shp_uf)):
        log(f"⚠️ UF shapefile NÃO encontrado: {shp_uf}")
        shp_uf = None
    if not (shp_mun and os.path.exists(shp_mun)):
        log(f"⚠️ MUN shapefile NÃO encontrado: {shp_mun}")
        shp_mun = None
    if not shp_uf and not shp_mun:
        log("⚠️ Nenhum shapefile válido informado — UF/município ficarão vazios.")
        return {}

    gdf_pts = gpd.GeoDataFrame(
        points_df.copy(),
        geometry=[Point(xy) for xy in zip(points_df["lon"], points_df["lat"])],
        crs="EPSG:4326"
    )

    uf_series = pd.Series("", index=gdf_pts.index, dtype=object)
    mun_series = pd.Series("", index=gdf_pts.index, dtype=object)

    if shp_uf:
        gdf_uf = gpd.read_file(shp_uf)
        gdf_uf = gpd.GeoDataFrame(gdf_uf)  # garante GeoDataFrame
        gdf_uf = gdf_uf.to_crs(epsg=4326) if gdf_uf.crs else gdf_uf.set_crs(epsg=4326)
        uf_col = pick_name_col(gdf_uf, True)
        log(f"[UF] usando coluna: {uf_col}")
        sj = gpd.sjoin(gdf_pts, gdf_uf[[uf_col, "geometry"]], how="left", predicate="within").sort_index()
        uf_series = sj[uf_col].reindex(gdf_pts.index).fillna("").astype(str)
        log(f"[UF] atribuídas: {(uf_series!='').sum():,} / {len(uf_series):,}")

    if shp_mun:
        gdf_mun = gpd.read_file(shp_mun)
        gdf_mun = gpd.GeoDataFrame(gdf_mun)
        gdf_mun = gdf_mun.to_crs(epsg=4326) if gdf_mun.crs else gdf_mun.set_crs(epsg=4326)
        mun_col = pick_name_col(gdf_mun, False)
        log(f"[MUN] usando coluna: {mun_col}")
        sj = gpd.sjoin(gdf_pts, gdf_mun[[mun_col, "geometry"]], how="left", predicate="within").sort_index()
        mun_series = sj[mun_col].reindex(gdf_pts.index).fillna("").astype(str)
        log(f"[MUN] atribuídos: {(mun_series!='').sum():,} / {len(mun_series):,}")

    result: Dict[Tuple[float, float], Tuple[str, str]] = {}
    for i, row in gdf_pts.iterrows():
        lat, lon = float(row["lat"]), float(row["lon"])
        result[key_latlon(lat, lon)] = (uf_series.loc[i], mun_series.loc[i])
    log(f"Geocodificação local concluída: {len(result):,} pontos (com reindex)")
    return result


# ===================== Cálculo de índices =====================

def annual_indices_for_point(series: np.ndarray,
                             wet_thr: float,
                             p95_thr: Optional[float],
                             r_heavy: Tuple[float, float]) -> Tuple[int, float, float, float, int, int, float, float]:
    """
    Calcula índices para um vetor 1D (dias do ano) já em mm/dia.
    Retorna: wet_days, sdii, rx1day, rx5day, r20mm, r50mm, r95ptot_mm, r95ptot_frac

    - wet_days: nº de dias com pr >= wet_thr  (NOVA definição de frequência)
    - sdii: média de pr apenas nos dias com pr >= wet_thr (NOVA definição de intensidade)
    """
    arr = np.asarray(series, dtype=np.float32)
    valid = np.isfinite(arr)
    if not np.any(valid):
        return 0, np.nan, np.nan, np.nan, 0, 0, np.nan, np.nan
    x = arr.copy()
    x[~valid] = 0.0

    # frequência (dias >= wet_thr)
    wet_mask = x >= wet_thr
    wet_days = int(wet_mask.sum())

    # INTENSIDADE (média condicionada aos dias >= wet_thr)
    sdii = float(x[wet_mask].mean()) if wet_days > 0 else np.nan

    # RX1day (máximo diário)
    rx1day = float(x.max()) if x.size > 0 else np.nan

    # RX5day (máximo acumulado em janela móvel de 5 dias)
    if x.size >= 5:
        k = np.ones(5, dtype=np.float32)
        acc5 = np.convolve(x, k, mode="valid")
        rx5day = float(acc5.max())
    else:
        rx5day = np.nan

    # contagem de dias muito chuvosos
    r20mm = int((x >= r_heavy[0]).sum())
    r50mm = int((x >= r_heavy[1]).sum())

    # R95pTOT (soma acima do limiar P95 do baseline)
    if p95_thr is not None:
        heavy = x > p95_thr
        r95ptot_mm = float(x[heavy].sum())
        tot = float(x.sum())
        r95ptot_frac = (r95ptot_mm / tot) if tot > 0 else np.nan
    else:
        r95ptot_mm = np.nan
        r95ptot_frac = np.nan

    return wet_days, sdii, rx1day, rx5day, r20mm, r50mm, r95ptot_mm, r95ptot_frac


def compute_p95_threshold_per_cell(pr_da: xr.DataArray,
                                   baseline: Optional[Tuple[int, int]],
                                   p95_wet_thr: float) -> Optional[np.ndarray]:
    """
    Calcula P95 por célula no baseline (sobre todos os dias desse período).
    Retorna um array 2D (ny, nx) com o limiar em mm/dia ou None se baseline=None.

    Observação: por padrão, p95_wet_thr = 1.0 mm/dia (ETCCDI) — independente do limiar de frequência/intensidade.
    """
    if baseline is None:
        return None
    years = pd.to_datetime(pr_da["time"].values).year
    mask = (years >= baseline[0]) & (years <= baseline[1])
    if not mask.any():
        log(f"⚠️ Nenhum dado no baseline {baseline[0]}–{baseline[1]} neste arquivo; r95ptot ficará NaN aqui.")
        return None
    sub = pr_da.sel(time=pr_da.time[mask])
    # dias chuvosos para percentis (ETCCDI): pr >= p95_wet_thr
    sub = sub.where(sub >= p95_wet_thr)
    thr = sub.quantile(0.95, dim="time", skipna=True)
    return np.asarray(thr.values, dtype=np.float32)


# ===================== Escrita =====================

def write_rows(grouped: xr.DataArray,
               lat2d: np.ndarray,
               lon2d: np.ndarray,
               scenario: str,
               var_name: str,
               out_path: str,
               wrote_header: List[bool],
               bbox_mask_flat: Optional[np.ndarray],
               p95_grid: Optional[np.ndarray],
               wet_thr: float,
               r_heavy: Tuple[float, float],
               loc_lookup: Optional[Dict[Tuple[float, float], Tuple[str, str]]]) -> int:

    lat_flat = lat2d.reshape(-1).astype("float64")
    lon_flat = ensure_lon_negpos180(lon2d.reshape(-1)).astype("float64")

    header = [
        "year","lat","lon","scenario","variable",
        "wet_days","sdii","rx1day","rx5day","r20mm","r50mm",
        "r95ptot_mm","r95ptot_frac","uf","municipio"
    ]

    mode = "a" if wrote_header[0] else "w"
    written = 0

    with open(out_path, mode, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not wrote_header[0]:
            w.writerow(header)
            wrote_header[0] = True

        # percorre cada ano
        for i, year in enumerate(grouped.year.values):
            yr = int(year)

            # "cube por ano": (time_in_year, ny, nx)
            year_da = grouped.isel(year=i)
            arr = np.asarray(year_da.values, dtype=np.float32)
            if arr.ndim != 3:
                raise ValueError("Esperava (time, y, x) após groupby('time.year').")
            t, ny, nx = arr.shape
            arr2 = arr.reshape(t, ny*nx).T  # (npix, t)

            if bbox_mask_flat is not None:
                pix_idx = np.where(bbox_mask_flat)[0]
            else:
                pix_idx = np.arange(ny*nx, dtype=int)

            # p95 por célula (se existir)
            p95_flat = p95_grid.reshape(-1) if p95_grid is not None else None

            for k in pix_idx:
                series = arr2[k, :]
                lat_k = float(lat_flat[k])
                lon_k = float(lon_flat[k])
                p95_thr = float(p95_flat[k]) if (p95_flat is not None and np.isfinite(p95_flat[k])) else None

                wet_days, sdii, rx1, rx5, r20, r50, r95mm, r95frac = annual_indices_for_point(
                    series, wet_thr=wet_thr, p95_thr=p95_thr, r_heavy=r_heavy
                )

                uf = mun = ""
                if loc_lookup:
                    uf, mun = loc_lookup.get(key_latlon(lat_k, lon_k), ("", ""))

                w.writerow([
                    yr, lat_k, lon_k, scenario, var_name,
                    wet_days, sdii, rx1, rx5, r20, r50,
                    r95mm, r95frac, uf, mun
                ])
                written += 1

    return written


# ===================== Pipeline =====================

def process_file(path: str,
                 out_path: str,
                 wrote_header: List[bool],
                 bbox: Optional[Tuple[float, float, float, float]],
                 copy_all: bool,
                 wet_thr: float,
                 r_heavy: Tuple[float, float],
                 baseline: Optional[Tuple[int, int]],
                 p95_wet_thr: float,
                 loc_lookup: Optional[Dict[Tuple[float, float], Tuple[str, str]]]) -> None:

    ds, tmpdir, _ = safe_open_with_copy(path, force_copy=copy_all)
    try:
        scenario = infer_scenario(path, ds)

        # pega pr diário com 2 dims espaciais + time
        if "pr" not in ds.data_vars:
            log(f"⚠️ Arquivo sem variável 'pr': {os.path.basename(path)} — pulando.")
            return
        da = ds["pr"]
        if "time" not in da.dims:
            log(f"⚠️ 'pr' sem dimensão tempo em {os.path.basename(path)} — pulando.")
            return

        # converte unidade
        da = convert_pr_to_mm_per_day(da)

        # prepara grade 2D
        lat_name, lon_name = guess_latlon_vars(ds)
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values
        lat2d, lon2d = coords_to_2d(lat_vals, lon_vals)

        # máscara BBOX
        bbox_mask_flat = build_in_bbox_mask(lat2d, lon2d, bbox) if bbox else None

        # calcula P95 no baseline por célula (usando p95_wet_thr, tipicamente 1.0 mm/dia)
        p95_grid = compute_p95_threshold_per_cell(da, baseline=baseline, p95_wet_thr=p95_wet_thr)

        # agrupa por ano, mas preserva o eixo diário para índices
        years = pd.to_datetime(da["time"].values).year
        groups: Dict[int, List[int]] = {}
        for i, tstamp in enumerate(da.time.values):
            y = int(pd.to_datetime(tstamp).year)
            groups.setdefault(y, []).append(i)

        years_sorted = sorted(groups.keys())
        stacked = []
        for y in years_sorted:
            sub = da.isel(time=xr.DataArray(groups[y], dims=["time"]))
            stacked.append(sub)

        grouped = xr.concat(stacked, dim="year")
        grouped = grouped.assign_coords(year=("year", years_sorted))

        # escreve linhas
        written = write_rows(
            grouped=grouped,
            lat2d=lat2d,
            lon2d=lon2d,
            scenario=scenario,
            var_name="pr",
            out_path=out_path,
            wrote_header=wrote_header,
            bbox_mask_flat=bbox_mask_flat,
            p95_grid=p95_grid,
            wet_thr=wet_thr,
            r_heavy=r_heavy,
            loc_lookup=loc_lookup
        )
        log(f"   ✔ {os.path.basename(path)}: {written:,} pontos×anos escritos.")

    finally:
        try:
            ds.close()
        except Exception:
            pass
        if tmpdir and os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir, ignore_errors=True)


# ===================== Main =====================

def main():
    ap = argparse.ArgumentParser(
        description="CORDEX • precipitação diária (pr) • índices de frequência e intensidade por ponto/ano → CSV único."
    )
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-dir", help="Pasta (recursivo) com .nc.")
    src.add_argument("--glob", help="Padrão glob, ex.: C:\\dados\\*.nc")

    ap.add_argument("--bbox", nargs=4, type=float, metavar=("LAT_MIN","LAT_MAX","LON_MIN","LON_MAX"),
                    default=[-33.75, 5.5, -74.0, -34.8],
                    help="BBOX (lon -180..180). Default = recorte grosso do Brasil.")
    ap.add_argument("--out", default="pr_freq_intensity.csv", help="CSV de saída.")
    ap.add_argument("--copy-all", action="store_true", help="Copiar cada .nc para /tmp antes de abrir (dribla locks).")

    # Parâmetros dos índices
    ap.add_argument("--freq-thr-mm", type=float, default=20.0,
                    help="Limiar T (mm) para frequência/intensidade: dias com pr >= T contam na frequência e entram na média de intensidade. Default 20.")
    ap.add_argument("--p95-wet-thr", type=float, default=1.0,
                    help="Limiar de 'dia chuvoso' (mm/dia) para computar o P95 do baseline (ETCCDI). Default 1.0.")
    ap.add_argument("--heavy20", type=float, default=20.0, help="Limiar p/ r20mm (mm). Default 20")
    ap.add_argument("--heavy50", type=float, default=50.0, help="Limiar p/ r50mm (mm). Default 50")
    ap.add_argument("--p95-baseline", type=str, default="2026-2035",
                    help="Período para P95 (ex.: '1995-2014' ou '2026-2035'). Use None p/ desativar.")

    # Geocodificação (opcional)
    ap.add_argument("--uf-shp", default=None, help="Shapefile de UFs do IBGE.")
    ap.add_argument("--mun-shp", default=None, help="Shapefile de municípios do IBGE.")

    args = ap.parse_args()
    bbox = tuple(args.bbox) if args.bbox else None
    baseline = None if (args.p95_baseline is None or str(args.p95_baseline).lower()=="none") else parse_years_range(args.p95_baseline)
    r_heavy = (float(args.heavy20), float(args.heavy50))

    # NOVO: limiar para frequência/intensidade
    wet_thr = float(args.freq_thr_mm)
    p95_wet_thr = float(args.p95_wet_thr)

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

    # geocodificação (opcional)
    loc_lookup: Optional[Dict[Tuple[float, float], Tuple[str, str]]] = None
    if (args.uf_shp or args.mun_shp):
        if not GEOS_OK:
            log("⚠️ Para UF/município, instale: pip install geopandas shapely rtree")
        else:
            # cataloga pontos únicos do BBOX em todos os arquivos
            coords: List[Tuple[float, float]] = []
            seen = set()
            for p in files:
                ds, tmpdir, _ = safe_open_with_copy(p, force_copy=args.copy_all)
                try:
                    la_name, lo_name = guess_latlon_vars(ds)
                    la_vals, lo_vals = ds[la_name].values, ds[lo_name].values
                    la2d, lo2d = coords_to_2d(la_vals, lo_vals)
                    mask = build_in_bbox_mask(la2d, lo2d, bbox) if bbox else np.ones(la2d.size, dtype=bool)
                    la_flat = la2d.reshape(-1)[mask]
                    lo_flat = ensure_lon_negpos180(lo2d.reshape(-1))[mask]
                    for la, lo in zip(la_flat, lo_flat):
                        k = key_latlon(la, lo)
                        if k not in seen:
                            seen.add(k)
                            coords.append((k[0], k[1]))
                finally:
                    try:
                        ds.close()
                    except Exception:
                        pass
                    if tmpdir and os.path.isdir(tmpdir):
                        shutil.rmtree(tmpdir, ignore_errors=True)
            pts_df = pd.DataFrame(coords, columns=["lat","lon"])
            log(f"Coordenadas únicas para geocodificação: {len(pts_df):,}")
            loc_lookup = geocode_points_with_shapes(pts_df, args.uf_shp, args.mun_shp)

    # CSV novo
    wrote_header = [False]
    if os.path.exists(args.out):
        os.remove(args.out)

    # processa cada arquivo
    for p in files:
        try:
            process_file(
                path=p,
                out_path=args.out,
                wrote_header=wrote_header,
                bbox=bbox,
                copy_all=args.copy_all,
                wet_thr=wet_thr,               
                r_heavy=r_heavy,
                baseline=baseline,
                p95_wet_thr=p95_wet_thr,      
                loc_lookup=loc_lookup
            )
        except Exception as e:
            log(f"⚠️ Falha em {os.path.basename(p)}: {e}")

    # escreve documento de “como foi calculado”
    doc_path = os.path.splitext(args.out)[0] + "_how_calculated.csv"
    with open(doc_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["campo","definicao","parametros"])
        # NOVO significado
        w.writerow(["wet_days","dias com pr >= T (mm/dia)","T=freq-thr-mm (default 20 mm/dia)"])
        w.writerow(["sdii","média de pr apenas nos dias com pr >= T", "T=freq-thr-mm (default 20 mm/dia)"])
        # Mantidos
        w.writerow(["rx1day","máxima precipitação diária no ano","mm"])
        w.writerow(["rx5day","máxima soma em qualquer janela móvel de 5 dias","mm em 5 dias"])
        w.writerow(["r20mm",f"nº de dias com pr >= {r_heavy[0]} mm",f"limiar={r_heavy[0]} mm"])
        w.writerow(["r50mm",f"nº de dias com pr >= {r_heavy[1]} mm",f"limiar={r_heavy[1]} mm"])
        # P95 separado com p95_wet_thr
        w.writerow(["r95ptot_mm","soma de pr acima do P95 do baseline",
                    f"baseline={(f'{baseline[0]}-{baseline[1]}' if baseline else 'None')}; p95_wet_thr={p95_wet_thr} mm/dia"])
        w.writerow(["r95ptot_frac","r95ptot_mm / soma anual","adimensional"])
        w.writerow(["scenario","inferido do nome do arquivo ou atributos","-"])
        w.writerow(["variable","'pr' (precipitação diária)","convertida p/ mm/dia"])

    log(f"✅ Concluído. CSV final: {os.path.abspath(args.out)}")
    log(f"📝 Doc: {os.path.abspath(doc_path)}")

if __name__ == "__main__":
    main()

