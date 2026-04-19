"""Adaptador :class:`ShapefileGeopandas` da porta :class:`ShapefileMunicipios`.

Carrega o shapefile do IBGE uma única vez em memória (via ``geopandas``) e
expõe consultas *point-in-polygon* vetorizadas com ``sjoin``. A instância é
singleton por processo — ``obter_shapefile`` em
``interfaces/dependencias.py`` usa ``functools.lru_cache`` para garantir.

Nenhum import de ``geopandas``/``shapely`` vaza para ``domain`` ou
``application``; o adaptador inteiro vive aqui. Os nomes de coluna do
shapefile do IBGE mudam entre versões (``CD_MUN`` vs. ``CD_GEOCMU`` vs.
``codigo_municipio``) — :func:`_detectar_colunas` resolve por preferência.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from climate_risk.domain.excecoes import ErroConfiguracao
from climate_risk.domain.portas.shapefile_municipios import LocalizacaoGeografica

logger = logging.getLogger(__name__)


_CANDIDATOS_ID = ("CD_MUN", "CD_GEOCMU", "codigo_municipio", "id")
_CANDIDATOS_UF = ("SIGLA_UF", "UF", "sigla")
_CANDIDATOS_NOME = ("NM_MUN", "NOME", "NM_MUNICIP")


@dataclass(frozen=True)
class _Colunas:
    id: str
    uf: str
    nome: str


def _detectar_colunas(colunas_disponiveis: list[str]) -> _Colunas:
    """Identifica as colunas de ID/UF/nome por preferência.

    Shapefiles do IBGE variam de versão: ``CD_MUN``/``NM_MUN`` no
    BR_Municipios_2022, ``CD_GEOCMU``/``NM_MUNICIP`` em versões antigas.
    Devolvemos a primeira coluna encontrada em cada lista de candidatos.
    Levanta :class:`ErroConfiguracao` quando alguma categoria falha —
    mensagem inclui as colunas disponíveis para diagnóstico.
    """
    disponiveis = set(colunas_disponiveis)

    def _primeira(candidatas: tuple[str, ...], categoria: str) -> str:
        for c in candidatas:
            if c in disponiveis:
                return c
        raise ErroConfiguracao(
            f"Shapefile sem coluna de {categoria}. Candidatos: {candidatas!r}. "
            f"Colunas disponíveis: {sorted(disponiveis)!r}."
        )

    return _Colunas(
        id=_primeira(_CANDIDATOS_ID, "ID"),
        uf=_primeira(_CANDIDATOS_UF, "UF"),
        nome=_primeira(_CANDIDATOS_NOME, "nome"),
    )


class ShapefileGeopandas:
    """Implementação :class:`ShapefileMunicipios` via ``geopandas``.

    Mantém o ``GeoDataFrame`` carregado em memória (~50 MB para
    BR_Municipios completo) pelo tempo de vida do processo. A chamada a
    :meth:`localizar_pontos` constrói um ``GeoDataFrame`` de pontos e faz
    ``sjoin(predicate="within")`` contra a malha — é a forma mais rápida
    que o ``geopandas`` oferece para centenas/milhares de pontos.
    """

    def __init__(self, caminho_shapefile: str) -> None:
        if not caminho_shapefile:
            raise ErroConfiguracao(
                "shapefile_mun_path não configurado. "
                "Defina CLIMATE_RISK_SHAPEFILE_MUN_PATH apontando para um .shp."
            )
        caminho = Path(caminho_shapefile)
        if not caminho.exists():
            raise ErroConfiguracao(f"Shapefile '{caminho_shapefile}' não encontrado no filesystem.")

        import geopandas as gpd  # import local: facilita mock e protege o domínio

        gdf = gpd.read_file(str(caminho))
        if gdf.crs is not None:
            epsg = gdf.crs.to_epsg()
            if epsg is None or epsg != 4326:
                gdf = gdf.to_crs(epsg=4326)

        self._colunas = _detectar_colunas(list(gdf.columns))
        self._gdf = gdf
        logger.info(
            "Shapefile carregado (%d municípios) de %s; colunas: %s",
            len(gdf),
            caminho,
            self._colunas,
        )

    def localizar_ponto(self, lat: float, lon: float) -> LocalizacaoGeografica | None:
        from shapely.geometry import Point

        ponto = Point(lon, lat)
        # ``contains(geom)`` devolve uma ``Series[bool]``; usamos isso porque
        # o GeoDataFrame já carregou um índice espacial interno no ``geopandas``.
        mascara = self._gdf.contains(ponto)
        if not bool(mascara.any()):
            return None
        indices = mascara[mascara].index
        linha = self._gdf.loc[indices[0]]
        return self._linha_para_localizacao(linha)

    def localizar_pontos(
        self, pontos: list[tuple[float, float]]
    ) -> list[LocalizacaoGeografica | None]:
        if not pontos:
            return []
        import geopandas as gpd
        from shapely.geometry import Point

        geometrias = [Point(lon, lat) for lat, lon in pontos]
        pontos_gdf = gpd.GeoDataFrame(
            {"_ordem": list(range(len(pontos)))},
            geometry=geometrias,
            crs="EPSG:4326",
        )
        # ``sjoin`` preserva o índice do GeoDataFrame da esquerda: podemos
        # reindexar para recuperar a ordem original das ``len(pontos)``
        # entradas — pontos fora de qualquer município ficam como ``NaN``.
        juncao = gpd.sjoin(pontos_gdf, self._gdf, how="left", predicate="within")
        # Se houver ambiguidade (ponto cai em mais de 1 polígono — raro em
        # bordas), mantemos a primeira ocorrência.
        juncao = juncao[~juncao.index.duplicated(keep="first")]

        resultado: list[LocalizacaoGeografica | None] = [None] * len(pontos)
        for idx in juncao.index:
            linha = juncao.loc[idx]
            ordem = int(linha["_ordem"])
            id_bruto = linha.get(self._colunas.id)
            if id_bruto is None or (isinstance(id_bruto, float) and _ehnan(id_bruto)):
                continue
            resultado[ordem] = self._linha_para_localizacao(linha)
        return resultado

    def _linha_para_localizacao(self, linha: Any) -> LocalizacaoGeografica:
        return LocalizacaoGeografica(
            municipio_id=int(linha[self._colunas.id]),
            uf=str(linha[self._colunas.uf]),
            nome_municipio=str(linha[self._colunas.nome]),
        )


def _ehnan(valor: float) -> bool:
    return valor != valor  # NaN é o único float que não é igual a si mesmo
