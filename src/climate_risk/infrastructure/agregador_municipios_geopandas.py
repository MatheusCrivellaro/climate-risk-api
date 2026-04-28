"""Adaptador :class:`AgregadorMunicipiosGeopandas`.

Implementa :class:`~climate_risk.domain.portas.agregador_espacial.AgregadorEspacial`
usando ``geopandas`` + shapefile de municípios do IBGE. A primeira chamada
para uma grade nova faz um ``sjoin(predicate="within")`` ponto-em-polígono
e persiste o mapeamento célula→município em parquet; chamadas seguintes
com a mesma grade recuperam o mapeamento do cache em microssegundos.

Toda a dependência de ``geopandas``/``shapely`` é encapsulada aqui — o
domínio permanece livre de I/O e de tipos específicos de biblioteca.
"""

from __future__ import annotations

import hashlib
import logging
import warnings
from collections.abc import Iterator
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Point

from climate_risk.domain.excecoes import (
    ErroConfiguracao,
    ErroGradeDesconhecida,
    ErroShapefileMunicipiosIndisponivel,
)

logger = logging.getLogger(__name__)

_CANDIDATOS_ID = ("CD_MUN", "cd_mun", "CD_GEOCMU", "GEOCODIGO", "id")


class AgregadorMunicipiosGeopandas:
    """Agregador espacial via shapefile IBGE + geopandas.

    Lê o shapefile uma vez na inicialização, reprojeta para EPSG:4326 se
    necessário e mantém em memória apenas as colunas ``municipio_id`` e
    ``geometry``. O cache em disco usa parquet e é indexado pelo hash das
    coordenadas da grade.
    """

    def __init__(
        self,
        shapefile_municipios: Path,
        cache_dir: Path,
    ) -> None:
        if not shapefile_municipios.exists():
            raise ErroShapefileMunicipiosIndisponivel(
                f"Shapefile não encontrado: {shapefile_municipios}"
            )
        cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_dir = cache_dir

        gdf = gpd.read_file(str(shapefile_municipios))
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        id_col = self._detectar_coluna_id_municipio(list(gdf.columns))
        gdf = gdf[[id_col, "geometry"]].rename(columns={id_col: "municipio_id"})
        # Normaliza municipio_id como string para evitar ambiguidades
        # (IBGE usa códigos com zero à esquerda em algumas representações).
        gdf["municipio_id"] = gdf["municipio_id"].astype(str)
        self._gdf_municipios = gdf
        logger.info(
            "Shapefile carregado (%d municípios) de %s",
            len(gdf),
            shapefile_municipios,
        )

    def agregar_por_municipio(
        self,
        dados: xr.DataArray,
        nome_variavel: str,
    ) -> pd.DataFrame:
        lat2d, lon2d = self._extrair_coordenadas_2d(dados)
        mapa = self._obter_mapa_celulas(lat2d, lon2d)
        return self._agregar_por_municipio_com_mapa(dados, nome_variavel, mapa)

    def iterar_por_municipio(
        self,
        dados: xr.DataArray,
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        """Streaming: yield ``(municipio_id, datas, serie_diaria)`` por município.

        Diferente de :meth:`agregar_por_municipio`, **não** monta DataFrame
        global. O caller é responsável por consumir as tuplas em ordem e
        liberar a memória entre iterações. Ver Slice 21 / ADR-013.
        """
        lat2d, lon2d = self._extrair_coordenadas_2d(dados)
        mapa = self._obter_mapa_celulas(lat2d, lon2d)
        if mapa.empty:
            return

        spatial_dims = [d for d in dados.dims if d != "time"]
        if len(spatial_dims) != 2:
            raise ErroGradeDesconhecida(
                f"DataArray precisa de 2 dimensões espaciais além de ``time``; "
                f"recebeu {spatial_dims!r}."
            )
        dim_y, dim_x = spatial_dims

        datas = pd.to_datetime(dados["time"].values).to_numpy()

        # Sort para garantir ordem determinística entre múltiplas chamadas —
        # iteradores paralelos (pr/tas/evap) precisam disso.
        municipio_ids = sorted(mapa["municipio_id"].unique())

        for municipio_id_str in municipio_ids:
            grupo = mapa[mapa["municipio_id"] == municipio_id_str]
            iy = np.asarray(grupo["iy"].to_numpy(), dtype=np.int64)
            ix = np.asarray(grupo["ix"].to_numpy(), dtype=np.int64)

            # Vectorized indexing: extrai apenas as células do município, sem
            # carregar o DataArray inteiro para a RAM (importante quando
            # ``dados`` é um xarray lazy / dask).
            sub = dados.isel(
                {
                    dim_y: xr.DataArray(iy, dims="cell"),
                    dim_x: xr.DataArray(ix, dims="cell"),
                }
            )
            with warnings.catch_warnings():
                # Coluna toda-NaN dispara aviso esperado — silencia.
                warnings.simplefilter("ignore", category=RuntimeWarning)
                serie = np.asarray(sub.mean(dim="cell", skipna=True).values)

            yield int(municipio_id_str), datas, serie

    @staticmethod
    def _detectar_coluna_id_municipio(colunas: list[str]) -> str:
        disponiveis = set(colunas)
        for candidato in _CANDIDATOS_ID:
            if candidato in disponiveis:
                return candidato
        raise ErroConfiguracao(
            f"Shapefile sem coluna de ID de município. "
            f"Candidatos aceitos: {_CANDIDATOS_ID!r}. "
            f"Colunas disponíveis: {sorted(disponiveis)!r}."
        )

    @staticmethod
    def _extrair_coordenadas_2d(
        dados: xr.DataArray,
    ) -> tuple[np.ndarray, np.ndarray]:
        coords = set(dados.coords)
        if "lat" in coords and "lon" in coords:
            lat = np.asarray(dados["lat"].values)
            lon = np.asarray(dados["lon"].values)
            if lat.ndim == 2 and lon.ndim == 2:
                return lat, lon
            if lat.ndim == 1 and lon.ndim == 1:
                lon2d, lat2d = np.meshgrid(lon, lat)
                return lat2d, lon2d
        raise ErroGradeDesconhecida(
            f"DataArray sem coordenadas lat/lon reconhecíveis. "
            f"Coordenadas disponíveis: {sorted(str(c) for c in coords)}"
        )

    def _obter_mapa_celulas(self, lat2d: np.ndarray, lon2d: np.ndarray) -> pd.DataFrame:
        chave = self._hash_grade(lat2d, lon2d)
        cache_path = self._cache_dir / f"mapa_celulas_{chave}.parquet"
        if cache_path.exists():
            mapa = pd.read_parquet(cache_path)
            logger.debug("Cache hit para grade %s (%d células)", chave, len(mapa))
            return mapa

        ny, nx = lat2d.shape
        lat_flat = lat2d.reshape(-1)
        # Normaliza longitudes para -180..180 (arquivos CORDEX às vezes vêm em 0..360).
        lon_flat = ((lon2d.reshape(-1) + 180.0) % 360.0) - 180.0
        iy_flat, ix_flat = np.unravel_index(np.arange(ny * nx), (ny, nx))

        pontos = gpd.GeoDataFrame(
            {"iy": iy_flat, "ix": ix_flat},
            geometry=[Point(lo, la) for lo, la in zip(lon_flat, lat_flat, strict=True)],
            crs="EPSG:4326",
        )
        juncao = gpd.sjoin(
            pontos,
            self._gdf_municipios,
            predicate="within",
            how="inner",
        )
        mapa = juncao[["iy", "ix", "municipio_id"]].reset_index(drop=True)
        # Em bordas raras, um ponto pode cair em mais de um polígono; mantemos
        # apenas a primeira associação por célula.
        mapa = mapa.drop_duplicates(subset=["iy", "ix"], keep="first").reset_index(drop=True)
        mapa.to_parquet(cache_path, index=False)
        logger.info(
            "Cache miss para grade %s: %d células mapeadas, %d municípios distintos",
            chave,
            len(mapa),
            mapa["municipio_id"].nunique(),
        )
        return mapa

    @staticmethod
    def _hash_grade(lat2d: np.ndarray, lon2d: np.ndarray) -> str:
        # Arredondamento a 5 casas evita hashs diferentes por ruído numérico
        # de ponto flutuante vindo de leituras distintas do mesmo arquivo.
        lat_r = np.round(lat2d, 5).tobytes()
        lon_r = np.round(lon2d, 5).tobytes()
        shape_str = f"{lat2d.shape[0]}x{lat2d.shape[1]}"
        h = hashlib.md5(shape_str.encode() + lat_r + lon_r).hexdigest()
        return h[:16]

    @staticmethod
    def _agregar_por_municipio_com_mapa(
        dados: xr.DataArray,
        nome_variavel: str,
        mapa: pd.DataFrame,
    ) -> pd.DataFrame:
        if mapa.empty:
            return pd.DataFrame(columns=["municipio_id", "data", "valor", "nome_variavel"])

        tempo = pd.to_datetime(dados["time"].values)
        valores = np.asarray(dados.values)
        if valores.ndim != 3:
            raise ErroGradeDesconhecida(
                f"DataArray precisa de 3 dimensões (time, y, x); recebeu {valores.ndim}."
            )
        n_tempo, _, nx = valores.shape
        valores_flat = valores.reshape(n_tempo, -1)

        registros: list[dict[str, object]] = []
        for municipio_id, grupo in mapa.groupby("municipio_id", sort=False):
            indices_flat = grupo["iy"].to_numpy() * nx + grupo["ix"].to_numpy()
            sub = valores_flat[:, indices_flat]
            # ``nanmean`` de uma coluna só-NaN dispara RuntimeWarning e devolve NaN;
            # silenciamos o aviso porque é caso esperado (célula mascarada).
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                medias = np.nanmean(sub, axis=1)
            for t, m in zip(tempo, medias, strict=True):
                registros.append(
                    {
                        "municipio_id": str(municipio_id),
                        "data": t,
                        "valor": float(m) if np.isfinite(m) else None,
                        "nome_variavel": nome_variavel,
                    }
                )

        return pd.DataFrame(registros, columns=["municipio_id", "data", "valor", "nome_variavel"])
