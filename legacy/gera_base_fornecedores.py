# -*- coding: utf-8 -*-
"""
Script completo:
- Baixa municípios do IBGE direto da API (com lat/lon)
- Normaliza nomes
- Match exato (cidade/UF)
- Fuzzy match quando não encontrar exato
- Gera CSV final: Cidade,Estado,Latitude,Longitude
"""

import re
import json
import time
import math
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from unidecode import unidecode
from rapidfuzz import process, fuzz
from shapely.geometry import shape
from shapely.ops import unary_union


# ===========================================================
# 1) SUA LISTA AQUI
# ===========================================================

RAW_LISTA = """
BENTO GONCALVES/RS
BENEVIDES/PA
OSASCO/SP
ITAQUAQUECETUBA/SP
ALUMINIO/SP
PORTO REAL/RJ
RECIFE/PE
HORTOLANDIA/SP
VARZEA PAULISTA/SP
CONTAGEM/MG
SUZANO/SP
EMBU DAS ARTES/SP
BARUERI/SP
PAULISTA/PE
""".strip()


# -*- coding: utf-8 -*-
"""
Gera CSV Cidade,Estado,Latitude,Longitude a partir da sua lista "CIDADE/UF",
fazendo match na base oficial do IBGE e calculando o centróide via malha (GeoJSON).

Dependências:
    pip install pandas requests unidecode rapidfuzz shapely

Como usar:
    1) Cole sua lista em RAW_LISTA, exatamente no formato CIDADE/UF (uma por linha).
    2) Rode: python gerar_coords_ibge.py
    3) Saída: cidades_geocodificadas.csv
"""




ARQ_SAIDA = "cidades_geocodificadas_fornecedores_final.csv"

# =========================
# 2) Normalização de nomes
# =========================
def normaliza_nome(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = (s.replace("’", "'")
           .replace("‘", "'")
           .replace("´", "'")
           .replace("`", "'"))
    s = unidecode(s.lower())
    s = s.replace("d'oeste", "doeste")
    s = s.replace("-", " ")
    s = s.replace("'", "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = _build_session()



def _get_nested(d, path):
    """Acessa d[path[0]][path[1]]... com tolerância a None/KeyError."""
    cur = d
    try:
        for p in path:
            if cur is None:
                return None
            cur = cur[p]
        return cur
    except (KeyError, TypeError):
        return None

def carregar_municipios_ibge():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()

    linhas = []
    for item in data:
        mid = item.get("id")
        nome = item.get("nome")

        # Tenta múltiplos caminhos possíveis para UF.sigla:
        uf = (
            _get_nested(item, ["microrregiao","mesorregiao","UF","sigla"])
            or _get_nested(item, ["regiao-imediata","regiao-intermediaria","UF","sigla"])
            or _get_nested(item, ["UF","sigla"])
        )

        if not uf or not nome or not mid:
            # pula itens malformados
            continue

        linhas.append((mid, nome, uf, normaliza_nome(nome)))

    df = pd.DataFrame(linhas, columns=["id", "nome", "uf", "nome_norm"])
    df["uf"] = df["uf"].str.upper()
    return df
# =========================
# 3) Carrega lista de municípios (IBGE)
# =========================


# =========================
# 4) Parsing da sua lista
# =========================
def parse_lista(raw):
    out = []
    for linha in raw.splitlines():
        linha = linha.strip()
        if not linha or "/" not in linha:
            continue
        cidade_raw, uf_raw = linha.rsplit("/", 1)
        out.append({
            "original": linha,
            "cidade_raw": cidade_raw.strip(),
            "uf": uf_raw.strip().upper(),
            "cidade_norm": normaliza_nome(cidade_raw)
        })
    return pd.DataFrame(out)

# =========================
# 5) Match exato + fuzzy
# =========================
def casar_cidade(df_in, df_ibge, limiar_fuzzy=90):
    registros = []
    for _, row in df_in.iterrows():
        uf = row["uf"]
        alvo = row["cidade_norm"]
        subset = df_ibge[df_ibge["uf"] == uf]

        # match exato
        exato = subset[subset["nome_norm"] == alvo]
        if len(exato) == 1:
            r = exato.iloc[0]
            registros.append((row["cidade_raw"], uf, r["id"], r["nome"]))
            continue

        # fuzzy
        nomes = subset["nome_norm"].tolist()
        if not nomes:
            registros.append((row["cidade_raw"], uf, None, None))
            continue
        melhor, score, pos = process.extractOne(alvo, nomes, scorer=fuzz.WRatio)
        if score >= limiar_fuzzy:
            r = subset.iloc[pos]
            registros.append((row["cidade_raw"], uf, r["id"], r["nome"]))
        else:
            registros.append((row["cidade_raw"], uf, None, None))
    cols = ["cidade_raw", "uf", "id_ibge", "nome_oficial"]
    return pd.DataFrame(registros, columns=cols)

# =========================
# 6) Baixa malha GeoJSON e calcula centróide
# =========================
_geo_cache = {}
_lock = threading.Lock()

def _get_geojson_municipio(mid: int):
    with _lock:
        if mid in _geo_cache:
            return _geo_cache[mid]

    url = f"https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{mid}?formato=application/vnd.geo+json"
    r = SESSION.get(url, timeout=60)
    if r.status_code != 200:
        return None

    gj = r.json()
    with _lock:
        _geo_cache[mid] = gj
    return gj

def _centroide_de_feature(feature):
    geom = shape(feature["geometry"])
    # O centróide pode cair fora em polígonos com buracos/multipolígonos; o representative_point() fica dentro.
    ponto = geom.representative_point()
    return ponto.y, ponto.x   # (lat, lon)

def obter_latlon_por_id(mid: int):
    gj = _get_geojson_municipio(mid)
    if not gj:
        return None, None

    # Pode vir como Feature ou FeatureCollection
    if gj.get("type") == "Feature":
        lat, lon = _centroide_de_feature(gj)
        return lat, lon
    elif gj.get("type") == "FeatureCollection":
        feats = gj.get("features", [])
        if not feats:
            return None, None
        # Se houver múltiplos, unir e pegar representative_point da união
        geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
        if not geoms:
            return None, None
        geom_unida = unary_union(geoms)
        pt = geom_unida.representative_point()
        return pt.y, pt.x
    else:
        return None, None

# =========================
# 7) Pipeline principal com paralelismo
# =========================
def processar(raw_lista: str, workers=6):
    print("Carregando municípios do IBGE...")
    df_ibge = carregar_municipios_ibge()
    print(f"Total IBGE: {len(df_ibge)}")

    df_in = parse_lista(raw_lista)
    print(f"Itens na sua lista: {len(df_in)}")

    df_match = casar_cidade(df_in, df_ibge, limiar_fuzzy=90)
    df_match["pos"] = range(len(df_match))

    resultados = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_row = {}

        for _, row in df_match.iterrows():
            mid = row["id_ibge"]
            if pd.isna(mid) or mid is None:
                resultados.append((row["pos"], row["cidade_raw"], row["uf"], "", ""))
            else:
                f = ex.submit(obter_latlon_por_id, int(mid))
                future_to_row[f] = row

        for f in as_completed(future_to_row):
            row = future_to_row[f]
            try:
                lat, lon = f.result()
                if lat is None or lon is None:
                    resultados.append((row["pos"], row["cidade_raw"], row["uf"], "", ""))
                else:
                    resultados.append((row["pos"], row["nome_oficial"], row["uf"], f"{lat:.8f}", f"{lon:.8f}"))
            except Exception:
                resultados.append((row["pos"], row["cidade_raw"], row["uf"], "", ""))

    df_out = pd.DataFrame(resultados, columns=["pos", "Cidade", "Estado", "Latitude", "Longitude"])
    df_out = df_out.sort_values("pos").drop(columns=["pos"])

    df_out.to_csv(ARQ_SAIDA, index=False, encoding="latin-1")

    print(f"\nArquivo gerado: {ARQ_SAIDA}")
    print(df_out.head(15).to_string(index=False))
    return df_out

if __name__ == "__main__":
    processar(RAW_LISTA, workers=8)















