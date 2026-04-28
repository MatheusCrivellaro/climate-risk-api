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
        # Cache em memória para evitar reler parquet em chamadas consecutivas
        # com a mesma grade — o pipeline de estresse hídrico (Slice 22)
        # consulta a mesma grade 2x (`municipios_mapeados` + `serie_de_municipio`).
        self._cache_memoria: dict[str, pd.DataFrame] = {}

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
        mapa = self._obter_mapa_para_dataarray(dados)
        return self._agregar_por_municipio_com_mapa(dados, nome_variavel, mapa)

    def iterar_por_municipio(
        self,
        dados: xr.DataArray,
        *,
        municipios_alvo: set[int] | None = None,
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        """Streaming: yield ``(municipio_id, datas, serie_diaria)`` por município.

        Diferente de :meth:`agregar_por_municipio`, **não** monta DataFrame
        global. O caller é responsável por consumir as tuplas em ordem e
        liberar a memória entre iterações. Ver Slice 21 / ADR-013.

        Slice 23 / ADR-015: aceita filtro opcional ``municipios_alvo`` para
        permitir iteração paralela de múltiplas variáveis (pr/tas/evap)
        com ``zip`` quando as grades têm coberturas distintas. Os 3
        iteradores recebem o mesmo conjunto (a interseção) e percorrem
        em ordem ascendente, garantindo sincronização determinística sem
        precisar chamar :meth:`serie_de_municipio` por município (que
        força um ``compute`` dask separado por chamada).
        """
        mapa = self._obter_mapa_para_dataarray(dados)
        if mapa.empty:
            return

        dim_y, dim_x = self._dimensoes_espaciais(dados)
        datas = pd.to_datetime(dados["time"].values).to_numpy()

        ids_mapeados = mapa["municipio_id"].unique()
        if municipios_alvo is not None:
            alvo_str = {str(m) for m in municipios_alvo}
            ids_para_iterar = [m for m in ids_mapeados if m in alvo_str]
        else:
            ids_para_iterar = list(ids_mapeados)

        # Sort para garantir ordem determinística entre múltiplas chamadas
        # (precondição para sincronizar iteradores via ``zip`` na Slice 23).
        municipio_ids = sorted(ids_para_iterar)

        for municipio_id_str in municipio_ids:
            grupo = mapa[mapa["municipio_id"] == municipio_id_str]
            serie = self._media_espacial(dados, grupo, dim_y, dim_x)
            yield int(municipio_id_str), datas, serie

    def municipios_mapeados(self, dados: xr.DataArray) -> set[int]:
        """Conjunto de IDs IBGE mapeados na grade de ``dados``.

        Operação leve: reusa o cache de mapeamento célula→município sem
        materializar séries diárias. Usado pelo pipeline de estresse
        hídrico (Slice 22) para calcular interseção entre as 3 variáveis
        antes de iterar.
        """
        mapa = self._obter_mapa_para_dataarray(dados)
        if mapa.empty:
            return set()
        return {int(municipio_id) for municipio_id in mapa["municipio_id"].unique()}

    def serie_de_municipio(
        self,
        dados: xr.DataArray,
        municipio_id: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Retorna ``(datas, serie_diaria)`` para um município específico.

        Reusa o cache de mapeamento. Levanta :class:`KeyError` se o
        município não está coberto por nenhuma célula da grade.

        Nota: para processamento em massa de muitos municípios, **não**
        chamar este método em loop — cada chamada dispara um ``compute``
        dask separado, perdendo a localidade do streaming. Use
        :meth:`iterar_por_municipio` com ``municipios_alvo`` (Slice 23)
        em vez disso.
        """
        mapa = self._obter_mapa_para_dataarray(dados)
        chave_str = str(municipio_id)
        grupo = mapa[mapa["municipio_id"] == chave_str]
        if grupo.empty:
            raise KeyError(f"Município {municipio_id} não está mapeado nesta grade")

        dim_y, dim_x = self._dimensoes_espaciais(dados)
        datas = pd.to_datetime(dados["time"].values).to_numpy()
        serie = self._media_espacial(dados, grupo, dim_y, dim_x)
        return datas, serie

    @staticmethod
    def _dimensoes_espaciais(dados: xr.DataArray) -> tuple[str, str]:
        spatial_dims = [d for d in dados.dims if d != "time"]
        if len(spatial_dims) != 2:
            raise ErroGradeDesconhecida(
                f"DataArray precisa de 2 dimensões espaciais além de ``time``; "
                f"recebeu {spatial_dims!r}."
            )
        return str(spatial_dims[0]), str(spatial_dims[1])

    @staticmethod
    def _media_espacial(
        dados: xr.DataArray,
        grupo: pd.DataFrame,
        dim_y: str,
        dim_x: str,
    ) -> np.ndarray:
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
            warnings.simplefilter("ignore", category=RuntimeWarning)
            return np.asarray(sub.mean(dim="cell", skipna=True).values)

    def _obter_mapa_para_dataarray(self, dados: xr.DataArray) -> pd.DataFrame:
        """Recupera mapa célula→município com cache em memória por grade."""
        lat2d, lon2d = self._extrair_coordenadas_2d(dados)
        chave = self._hash_grade(lat2d, lon2d)
        if chave in self._cache_memoria:
            return self._cache_memoria[chave]
        mapa = self._obter_mapa_celulas(lat2d, lon2d)
        self._cache_memoria[chave] = mapa
        return mapa

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
