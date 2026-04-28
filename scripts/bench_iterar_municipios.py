"""Benchmark do gargalo de performance do pipeline streaming (Slice 24).

Reproduz com fixtures sintéticas o cenário real onde 3 grades dask-backed
(pr/tas/evspsbl) são iteradas município a município. O objetivo é
comparar 3 padrões de consumo do agregador espacial:

- **Slice 21**: zip de iteradores SEM filtro. Cada iterador percorre seu
  próprio ``mapa["municipio_id"].unique()``. Funciona apenas quando os 3
  conjuntos coincidem; usado como baseline "ideal" no benchmark.
- **Slice 22**: loop por município com 3 chamadas a ``serie_de_municipio``
  por município (uma por variável).
- **Slice 23**: zip de iteradores filtrados pela interseção
  (``municipios_alvo``). Cada iterador percorre o mesmo conjunto em
  ordem ascendente.

Bônus:

- **Materializado**: carrega ``dados.values`` uma única vez por
  variável, depois indexa via NumPy puro (replica o padrão de
  ``_agregar_por_municipio_com_mapa``). Hipótese de fix para a Slice 25.

Cada estratégia roda 2 vezes: uma sem profiler para tempo wallclock
limpo e outra com ``cProfile`` ativo. Profiles são salvos em
``tests/perf/profile_<estrategia>.txt`` com top-30 funções por tempo
cumulativo.

Uso::

    uv run python scripts/bench_iterar_municipios.py
    uv run python scripts/bench_iterar_municipios.py --municipios=200 --anos=10
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import time
import warnings
from collections.abc import Iterator
from pathlib import Path

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr

RAIZ_REPO = Path(__file__).resolve().parent.parent
DESTINO_PROFILES = RAIZ_REPO / "tests" / "perf"


# ---------------------------------------------------------------------------
# Fixtures sintéticas
# ---------------------------------------------------------------------------


def criar_grades_sinteticas(
    *,
    n_municipios_total: int = 100,
    n_anos: int = 5,
    seed: int = 42,
    chunk_time: int = 365,
) -> tuple[
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Cria 3 DataArrays dask-backed e seus mapeamentos célula→município.

    A grade espacial é uma malha regular ``ny x nx`` onde
    ``ny * nx ~ n_municipios_total * 16`` (média de 16 células por
    município, próxima da realidade CORDEX/IBGE em pequenas regiões).

    pr e tas compartilham o mesmo mapeamento (mesmo modelo climático).
    evap usa uma grade com ``~80%`` dos municípios em comum + ``~10%``
    municípios próprios, simulando a divergência observada em produção
    (ADR-014).

    Returns:
        ``(da_pr, da_tas, da_evap, mapa_pr, mapa_tas, mapa_evap)``
    """
    rng = np.random.default_rng(seed)

    n_dias = n_anos * 365
    # Grade ~ retângulo proporcional a sqrt(n_celulas)
    n_celulas_alvo = n_municipios_total * 16
    nx = int(np.ceil(np.sqrt(n_celulas_alvo)))
    ny = int(np.ceil(n_celulas_alvo / nx))

    tempo = pd.date_range("2000-01-01", periods=n_dias, freq="D")
    lat_1d = np.linspace(-30.0, -10.0, ny).astype(np.float64)
    lon_1d = np.linspace(-60.0, -40.0, nx).astype(np.float64)

    chunk_y = max(1, ny // 4)
    chunk_x = max(1, nx // 4)

    def _criar_da(nome: str) -> xr.DataArray:
        arr = (
            da.random.default_rng(seed=hash(nome) % (2**32))
            .standard_normal(
                size=(n_dias, ny, nx),
                chunks=(chunk_time, chunk_y, chunk_x),
            )
            .astype(np.float32)
        )
        return xr.DataArray(
            arr,
            dims=("time", "y", "x"),
            coords={"time": tempo, "lat": (("y",), lat_1d), "lon": (("x",), lon_1d)},
            name=nome,
        )

    da_pr = _criar_da("pr")
    da_tas = _criar_da("tas")
    da_evap = _criar_da("evap")

    # Mapa célula→município. Cada célula recebe um ID; múltiplas células
    # podem mapear para o mesmo município (média espacial).
    iy_grid, ix_grid = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    iy_flat = iy_grid.reshape(-1)
    ix_flat = ix_grid.reshape(-1)

    # Distribui aleatoriamente células entre municípios pr/tas
    municipios_pr_tas = rng.integers(
        low=1100000,
        high=1100000 + n_municipios_total,
        size=iy_flat.size,
    )
    mapa_pr = pd.DataFrame(
        {
            "iy": iy_flat,
            "ix": ix_flat,
            "municipio_id": municipios_pr_tas.astype(str),
        }
    )
    mapa_tas = mapa_pr.copy()

    # evap: 80% das células mantém o mesmo município, 20% recebem
    # municípios "próprios" (alguns são novos, alguns saem do conjunto).
    rotulos_evap = municipios_pr_tas.copy()
    mascara_realocar = rng.random(rotulos_evap.size) < 0.20
    novos_ids = rng.integers(
        low=1200000,
        high=1200000 + n_municipios_total // 5,
        size=int(mascara_realocar.sum()),
    )
    rotulos_evap[mascara_realocar] = novos_ids
    mapa_evap = pd.DataFrame(
        {
            "iy": iy_flat,
            "ix": ix_flat,
            "municipio_id": rotulos_evap.astype(str),
        }
    )

    return da_pr, da_tas, da_evap, mapa_pr, mapa_tas, mapa_evap


# ---------------------------------------------------------------------------
# "Agregador" minimalista: replica o caminho quente do adapter de produção
# (Slice 21/22/23) sem depender de geopandas/shapefile.
# ---------------------------------------------------------------------------


class AgregadorFake:
    """Replica o caminho quente do AgregadorMunicipiosGeopandas.

    Recebe o mapa célula→município pronto (em produção é construído via
    ``sjoin`` do geopandas). Os métodos abaixo fazem **exatamente** o
    mesmo trabalho dos métodos homônimos em
    :mod:`infrastructure.agregador_municipios_geopandas` — copiado por
    simetria, não importado, para não acoplar o benchmark à evolução do
    adapter.
    """

    def __init__(self, mapa: pd.DataFrame, dim_y: str = "y", dim_x: str = "x") -> None:
        self._mapa = mapa
        self._dim_y = dim_y
        self._dim_x = dim_x

    @staticmethod
    def _media_espacial(
        dados: xr.DataArray,
        grupo: pd.DataFrame,
        dim_y: str,
        dim_x: str,
    ) -> np.ndarray:
        iy = np.asarray(grupo["iy"].to_numpy(), dtype=np.int64)
        ix = np.asarray(grupo["ix"].to_numpy(), dtype=np.int64)
        sub = dados.isel(
            {
                dim_y: xr.DataArray(iy, dims="cell"),
                dim_x: xr.DataArray(ix, dims="cell"),
            }
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            return np.asarray(sub.mean(dim="cell", skipna=True).values)

    def municipios_mapeados(self) -> set[int]:
        return {int(m) for m in self._mapa["municipio_id"].unique()}

    def iterar_por_municipio(
        self,
        dados: xr.DataArray,
        *,
        municipios_alvo: set[int] | None = None,
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        mapa = self._mapa
        if mapa.empty:
            return
        datas = pd.to_datetime(dados["time"].values).to_numpy()
        ids_mapeados = mapa["municipio_id"].unique()
        if municipios_alvo is not None:
            alvo_str = {str(m) for m in municipios_alvo}
            ids_para_iterar = [m for m in ids_mapeados if m in alvo_str]
        else:
            ids_para_iterar = list(ids_mapeados)
        municipio_ids = sorted(ids_para_iterar)

        for municipio_id_str in municipio_ids:
            grupo = mapa[mapa["municipio_id"] == municipio_id_str]
            serie = self._media_espacial(dados, grupo, self._dim_y, self._dim_x)
            yield int(municipio_id_str), datas, serie

    def serie_de_municipio(
        self,
        dados: xr.DataArray,
        municipio_id: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        chave_str = str(municipio_id)
        grupo = self._mapa[self._mapa["municipio_id"] == chave_str]
        if grupo.empty:
            raise KeyError(f"Município {municipio_id} não está mapeado")
        datas = pd.to_datetime(dados["time"].values).to_numpy()
        serie = self._media_espacial(dados, grupo, self._dim_y, self._dim_x)
        return datas, serie


# ---------------------------------------------------------------------------
# Estratégias de consumo
# ---------------------------------------------------------------------------


def _consumir(serie_pr: np.ndarray, serie_tas: np.ndarray, serie_evap: np.ndarray) -> float:
    """Mock simples do cálculo de índices anuais.

    Faz uma redução aritmética sobre as 3 séries para garantir que o
    iterator avança e nada é otimizado fora pelo Python. Não tenta
    reproduzir ``calcular_indices_anuais_estresse_hidrico`` — o objetivo
    é exclusivamente medir a obtenção das séries, não o cálculo.
    """
    return float(np.nansum(serie_pr) + np.nansum(serie_tas) + np.nansum(serie_evap))


def estrategia_slice_21(
    ag_pr: AgregadorFake,
    ag_tas: AgregadorFake,
    ag_evap: AgregadorFake,
    da_pr: xr.DataArray,
    da_tas: xr.DataArray,
    da_evap: xr.DataArray,
    municipios_comuns: set[int],
) -> int:
    """Padrão Slice 21: zip de iteradores SEM filtro.

    Premissa do Slice 21 (grades coincidentes) é fingida aqui
    restringindo previamente cada mapa à interseção; o caminho quente
    do iterator continua sendo
    ``mapa.unique() → sort → loop → isel/mean/values``, agora sem o
    kwarg ``municipios_alvo``.
    """
    alvo_str = {str(m) for m in municipios_comuns}
    ag_pr_red = AgregadorFake(
        ag_pr._mapa[ag_pr._mapa["municipio_id"].isin(alvo_str)].reset_index(drop=True)
    )
    ag_tas_red = AgregadorFake(
        ag_tas._mapa[ag_tas._mapa["municipio_id"].isin(alvo_str)].reset_index(drop=True)
    )
    ag_evap_red = AgregadorFake(
        ag_evap._mapa[ag_evap._mapa["municipio_id"].isin(alvo_str)].reset_index(drop=True)
    )
    iter_pr = ag_pr_red.iterar_por_municipio(da_pr)
    iter_tas = ag_tas_red.iterar_por_municipio(da_tas)
    iter_evap = ag_evap_red.iterar_por_municipio(da_evap)

    n = 0
    for (mun_pr, _, serie_pr), (_, _, serie_tas), (_, _, serie_evap) in zip(
        iter_pr, iter_tas, iter_evap, strict=True
    ):
        _consumir(serie_pr, serie_tas, serie_evap)
        if mun_pr not in municipios_comuns:
            raise RuntimeError("Inconsistência: município fora da interseção")
        n += 1
    return n


def estrategia_slice_22(
    ag_pr: AgregadorFake,
    ag_tas: AgregadorFake,
    ag_evap: AgregadorFake,
    da_pr: xr.DataArray,
    da_tas: xr.DataArray,
    da_evap: xr.DataArray,
    municipios_comuns: set[int],
) -> int:
    """Padrão Slice 22: loop por município, 3 chamadas a serie_de_municipio."""
    n = 0
    for municipio_id in sorted(municipios_comuns):
        _, serie_pr = ag_pr.serie_de_municipio(da_pr, municipio_id)
        _, serie_tas = ag_tas.serie_de_municipio(da_tas, municipio_id)
        _, serie_evap = ag_evap.serie_de_municipio(da_evap, municipio_id)
        _consumir(serie_pr, serie_tas, serie_evap)
        n += 1
    return n


def estrategia_slice_23(
    ag_pr: AgregadorFake,
    ag_tas: AgregadorFake,
    ag_evap: AgregadorFake,
    da_pr: xr.DataArray,
    da_tas: xr.DataArray,
    da_evap: xr.DataArray,
    municipios_comuns: set[int],
) -> int:
    """Padrão Slice 23: zip de iteradores COM ``municipios_alvo``."""
    iter_pr = ag_pr.iterar_por_municipio(da_pr, municipios_alvo=municipios_comuns)
    iter_tas = ag_tas.iterar_por_municipio(da_tas, municipios_alvo=municipios_comuns)
    iter_evap = ag_evap.iterar_por_municipio(da_evap, municipios_alvo=municipios_comuns)

    n = 0
    for (_, _, serie_pr), (_, _, serie_tas), (_, _, serie_evap) in zip(
        iter_pr, iter_tas, iter_evap, strict=True
    ):
        _consumir(serie_pr, serie_tas, serie_evap)
        n += 1
    return n


def estrategia_materializado(
    ag_pr: AgregadorFake,
    ag_tas: AgregadorFake,
    ag_evap: AgregadorFake,
    da_pr: xr.DataArray,
    da_tas: xr.DataArray,
    da_evap: xr.DataArray,
    municipios_comuns: set[int],
) -> int:
    """Padrão hipotético Slice 25: materializa ``.values`` UMA vez por variável.

    Replica o padrão de ``_agregar_por_municipio_com_mapa`` (legacy
    eager): faz 1 ``compute`` dask por variável (3 total), depois
    indexa via NumPy puro por município. Custo de RAM extra:
    ``3 * n_dias * ny * nx * 4 bytes`` (float32). Em produção (25 anos
    * 50 * 50): ~750 MB — viável.
    """
    valores_pr = np.asarray(da_pr.values)
    valores_tas = np.asarray(da_tas.values)
    valores_evap = np.asarray(da_evap.values)

    n_tempo, _, nx = valores_pr.shape
    flat_pr = valores_pr.reshape(n_tempo, -1)
    flat_tas = valores_tas.reshape(n_tempo, -1)
    flat_evap = valores_evap.reshape(n_tempo, -1)

    alvo_str = {str(m) for m in municipios_comuns}

    # Pré-agrupa cada mapa (groupby uma vez é O(N), batendo o filtro
    # repetido O(N×K) que o iterator de produção faz).
    grupos_pr = {
        municipio_id: grupo
        for municipio_id, grupo in ag_pr._mapa.groupby("municipio_id", sort=False)
        if municipio_id in alvo_str
    }
    grupos_tas = {
        municipio_id: grupo
        for municipio_id, grupo in ag_tas._mapa.groupby("municipio_id", sort=False)
        if municipio_id in alvo_str
    }
    grupos_evap = {
        municipio_id: grupo
        for municipio_id, grupo in ag_evap._mapa.groupby("municipio_id", sort=False)
        if municipio_id in alvo_str
    }

    n = 0
    for municipio_id in sorted(alvo_str):
        for flat, grupos in (
            (flat_pr, grupos_pr),
            (flat_tas, grupos_tas),
            (flat_evap, grupos_evap),
        ):
            grupo = grupos[municipio_id]
            indices_flat = grupo["iy"].to_numpy() * nx + grupo["ix"].to_numpy()
            sub = flat[:, indices_flat]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                _ = np.nanmean(sub, axis=1)
        n += 1
    return n


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


ESTRATEGIAS = {
    "slice_21": estrategia_slice_21,
    "slice_22": estrategia_slice_22,
    "slice_23": estrategia_slice_23,
    "materializado": estrategia_materializado,
}


def _construir_contexto(args: argparse.Namespace) -> dict[str, object]:
    da_pr, da_tas, da_evap, mapa_pr, mapa_tas, mapa_evap = criar_grades_sinteticas(
        n_municipios_total=args.municipios,
        n_anos=args.anos,
        chunk_time=args.chunk_time,
    )

    municipios_pr = {int(m) for m in mapa_pr["municipio_id"].unique()}
    municipios_tas = {int(m) for m in mapa_tas["municipio_id"].unique()}
    municipios_evap = {int(m) for m in mapa_evap["municipio_id"].unique()}
    municipios_comuns = municipios_pr & municipios_tas & municipios_evap

    return {
        "da_pr": da_pr,
        "da_tas": da_tas,
        "da_evap": da_evap,
        "mapa_pr": mapa_pr,
        "mapa_tas": mapa_tas,
        "mapa_evap": mapa_evap,
        "municipios_comuns": municipios_comuns,
        "n_municipios_pr": len(municipios_pr),
        "n_municipios_tas": len(municipios_tas),
        "n_municipios_evap": len(municipios_evap),
    }


def _construir_agregadores(
    contexto: dict[str, object],
) -> tuple[AgregadorFake, AgregadorFake, AgregadorFake]:
    return (
        AgregadorFake(contexto["mapa_pr"]),  # type: ignore[arg-type]
        AgregadorFake(contexto["mapa_tas"]),  # type: ignore[arg-type]
        AgregadorFake(contexto["mapa_evap"]),  # type: ignore[arg-type]
    )


def _rodar_estrategia(
    nome: str,
    func,
    contexto: dict[str, object],
) -> tuple[float, int]:
    """Roda a estratégia uma vez e mede tempo wallclock.

    Reconstrói os agregadores em cada chamada para garantir que caches
    internos não interferem na medição (paridade entre execuções).
    """
    ag_pr, ag_tas, ag_evap = _construir_agregadores(contexto)
    inicio = time.perf_counter()
    n = func(
        ag_pr,
        ag_tas,
        ag_evap,
        contexto["da_pr"],  # type: ignore[arg-type]
        contexto["da_tas"],  # type: ignore[arg-type]
        contexto["da_evap"],  # type: ignore[arg-type]
        contexto["municipios_comuns"],  # type: ignore[arg-type]
    )
    delta = time.perf_counter() - inicio
    return delta, n


def _rodar_com_cprofile(
    nome: str,
    func,
    contexto: dict[str, object],
    destino: Path,
) -> None:
    ag_pr, ag_tas, ag_evap = _construir_agregadores(contexto)
    profiler = cProfile.Profile()
    profiler.enable()
    func(
        ag_pr,
        ag_tas,
        ag_evap,
        contexto["da_pr"],  # type: ignore[arg-type]
        contexto["da_tas"],  # type: ignore[arg-type]
        contexto["da_evap"],  # type: ignore[arg-type]
        contexto["municipios_comuns"],  # type: ignore[arg-type]
    )
    profiler.disable()

    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8") as f:
        f.write(f"# cProfile -- estrategia={nome}\n")
        f.write("# Top 30 funcoes por tempo cumulativo\n\n")
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats("cumulative")
        stats.print_stats(30)
        f.write("\n# Top 30 funcoes por tempo total (self-time)\n\n")
        stats.sort_stats("tottime")
        stats.print_stats(30)


def _formatar_tabela(resultados: list[tuple[str, float, int]]) -> str:
    linhas = [
        "Estratégia              | Tempo wallclock | Municípios | Mun/seg",
        "------------------------|-----------------|------------|---------",
    ]
    for nome, tempo, n in resultados:
        rate = n / tempo if tempo > 0 else 0.0
        linhas.append(f"{nome:<23} | {tempo:>13.3f} s | {n:>10d} | {rate:>7.1f}")
    return "\n".join(linhas)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--municipios", type=int, default=100)
    parser.add_argument("--anos", type=int, default=5)
    parser.add_argument(
        "--chunk-time",
        type=int,
        default=365,
        help="Tamanho do chunk dask na dimensão time (default 365 dias).",
    )
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="Pula o passo de cProfile (só mede tempo wallclock).",
    )
    parser.add_argument(
        "--estrategias",
        nargs="+",
        choices=list(ESTRATEGIAS.keys()),
        default=list(ESTRATEGIAS.keys()),
    )
    args = parser.parse_args()

    print(
        f"=== Bench: {args.municipios} municípios, {args.anos} anos, "
        f"chunks_time={args.chunk_time} ==="
    )
    print("Construindo fixtures sintéticas...")
    contexto = _construir_contexto(args)
    print(
        f"Municípios pr={contexto['n_municipios_pr']} "
        f"tas={contexto['n_municipios_tas']} "
        f"evap={contexto['n_municipios_evap']} "
        f"interseção={len(contexto['municipios_comuns'])}"  # type: ignore[arg-type]
    )

    resultados: list[tuple[str, float, int]] = []
    for nome in args.estrategias:
        func = ESTRATEGIAS[nome]
        print(f"\n[{nome}] medindo wallclock...")
        tempo, n = _rodar_estrategia(nome, func, contexto)
        print(f"[{nome}] {tempo:.3f}s  ({n} municípios)")
        resultados.append((nome, tempo, n))

        if not args.skip_profile:
            destino = DESTINO_PROFILES / f"profile_{nome}.txt"
            print(f"[{nome}] rodando cProfile -> {destino.relative_to(RAIZ_REPO)}")
            _rodar_com_cprofile(nome, func, contexto, destino)

    print("\n=== Resultado ===")
    print(_formatar_tabela(resultados))
    if not args.skip_profile:
        print(f"\nProfiles salvos em {DESTINO_PROFILES.relative_to(RAIZ_REPO)}/")


if __name__ == "__main__":
    main()
