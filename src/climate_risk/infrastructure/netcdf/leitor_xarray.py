"""Adaptador :class:`LeitorXarray` — implementação de :class:`LeitorNetCDF`.

Porta as quatro funções do script legado ``gera_pontos_fornecedores.py``
(código legado removido na Slice 12) para helpers privados deste módulo,
conforme ADR-001:

- ``open_nc_multi``      -> :func:`_abrir_multi_engine`
- ``safe_open_with_copy``-> :func:`_abrir_com_fallback_copia`
- ``guess_latlon_vars``  -> :func:`_identificar_coords_lat_lon`
- ``infer_scenario``     -> :func:`_inferir_cenario`

A classe pública :class:`LeitorXarray` encapsula TODA a particularidade
de NetCDF/xarray nesta camada. O retorno é
:class:`~climate_risk.domain.entidades.dados_climaticos.DadosClimaticos`
— dataclass puro com arrays ``numpy`` e tipos primitivos (ver ADR-005).

**I/O bloqueante:** ``xarray.open_dataset`` e operações que disparam leitura
do disco são síncronas. Para não travar o event loop em contexto ``async``,
este módulo delega chamadas bloqueantes para um thread via
``asyncio.to_thread`` (disponível desde Python 3.9; o projeto exige 3.12+).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import tempfile
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos
from climate_risk.domain.espacial.grade import coords_to_2d
from climate_risk.domain.espacial.longitude import ensure_lon_negpos180
from climate_risk.domain.excecoes import (
    ErroArquivoNCNaoEncontrado,
    ErroCoordenadasLatLonAusentes,
    ErroDimensaoTempoAusente,
    ErroLeituraNetCDF,
    ErroVariavelAusente,
)
from climate_risk.domain.unidades.conversores import ConversorPrecipitacao

__all__ = [
    "LeitorXarray",
    "_abrir_com_fallback_copia",
    "_abrir_multi_engine",
    "_extrair_calendario",
    "_identificar_coords_lat_lon",
    "_inferir_cenario",
]

logger = logging.getLogger(__name__)

_SCENARIO_RE = re.compile(r"(rcp\d{2}|ssp\d{3})", re.IGNORECASE)

# Engines tentados pelo adaptador, em ordem. ``None`` deixa o xarray
# escolher o backend padrão (equivalente ao "auto" do legado).
_ENGINES: tuple[str | None, ...] = ("netcdf4", "h5netcdf", "scipy", None)


def _abrir_multi_engine(caminho: str) -> xr.Dataset:
    """Tenta abrir o NetCDF com múltiplas engines, preferindo ``use_cftime``.

    Porte fiel de ``open_nc_multi`` do legado. Emite um ``warning`` filtrado
    porque o xarray moderno deprecou ``use_cftime`` como kwarg direto — o
    comportamento em runtime ainda funciona nas versões suportadas (ADR-001,
    preservação de comportamento do legado).

    Raises:
        ErroLeituraNetCDF: se nenhuma combinação (engine, use_cftime)
            conseguiu abrir o arquivo.
    """
    last_err: Exception | None = None
    for eng in _ENGINES:
        for usar_cftime in (True, False):
            try:
                kwargs: dict[str, Any] = {
                    "engine": eng,
                    "decode_times": True,
                    "mask_and_scale": True,
                }
                if usar_cftime:
                    kwargs["use_cftime"] = True
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", FutureWarning)
                    ds = xr.open_dataset(caminho, **kwargs)
                _ = list(ds.sizes)  # força leitura do header
                logger.info(
                    "NetCDF aberto (engine=%s, use_cftime=%s): %s",
                    eng or "auto",
                    usar_cftime,
                    os.path.basename(caminho),
                )
                return ds
            except TypeError as erro:
                if "unexpected keyword argument 'use_cftime'" in str(erro):
                    # Versão antiga do xarray — tenta sem o kwarg.
                    continue
                last_err = erro
            except Exception as erro:
                last_err = erro
                logger.warning(
                    "Falha ao abrir NetCDF (engine=%s, use_cftime=%s): %s: %s",
                    eng,
                    usar_cftime,
                    type(erro).__name__,
                    erro,
                )
    detalhe = str(last_err) if last_err else "nenhuma engine disponível."
    raise ErroLeituraNetCDF(caminho=caminho, detalhe=detalhe)


def _abrir_com_fallback_copia(
    caminho: str,
    forcar_copia: bool,
) -> tuple[xr.Dataset, str | None]:
    """Abre com fallback para ``tempfile`` em caso de lock/permissão.

    Porte fiel de ``safe_open_with_copy`` do legado.

    Returns:
        Tupla ``(dataset, tmpdir)``. ``tmpdir`` é ``None`` quando nenhuma
        cópia foi necessária; caso contrário, o chamador é responsável por
        remover o diretório após fechar o dataset.
    """
    if forcar_copia:
        tmpdir = tempfile.mkdtemp(prefix="nc_copy_")
        destino = os.path.join(tmpdir, os.path.basename(caminho))
        shutil.copy2(caminho, destino)
        return _abrir_multi_engine(destino), tmpdir
    try:
        return _abrir_multi_engine(caminho), None
    except PermissionError:
        logger.warning(
            "Permissão negada em %s; copiando para diretório temporário.",
            os.path.basename(caminho),
        )
        tmpdir = tempfile.mkdtemp(prefix="nc_copy_")
        destino = os.path.join(tmpdir, os.path.basename(caminho))
        shutil.copy2(caminho, destino)
        return _abrir_multi_engine(destino), tmpdir


def _identificar_coords_lat_lon(caminho: str, ds: xr.Dataset) -> tuple[str, str]:
    """Encontra nomes de ``lat``/``lon`` em ``coords`` ou ``variables``.

    Porte fiel de ``guess_latlon_vars`` do legado — preserva a ordem de
    tentativa (``lat``, ``latitude``, ``y``) combinada com
    (``lon``, ``longitude``, ``x``).

    Raises:
        ErroCoordenadasLatLonAusentes: quando nenhum par reconhecido existe.
    """
    for la in ("lat", "latitude", "y"):
        for lo in ("lon", "longitude", "x"):
            if la in ds.coords and lo in ds.coords:
                return la, lo
            if la in ds.variables and lo in ds.variables:
                return la, lo
    raise ErroCoordenadasLatLonAusentes(
        caminho=caminho,
        detalhe="nenhum par (lat/lon) reconhecido em coords ou variables.",
    )


def _inferir_cenario(caminho: str, ds: xr.Dataset) -> str:
    """Identifica o cenário (``rcp``/``ssp``) do arquivo.

    Porte fiel de ``infer_scenario`` do legado. Prioriza match regex no
    basename; se falhar, tenta atributos globais ``experiment_id``,
    ``scenario`` e ``experiment`` (nessa ordem). Retorna ``"unknown"``
    quando nada é encontrado.
    """
    nome = os.path.basename(caminho)
    match = _SCENARIO_RE.search(nome)
    if match:
        return match.group(1).lower()
    for chave in ("experiment_id", "scenario", "experiment"):
        valor = str(ds.attrs.get(chave, "")).strip().lower()
        if valor:
            return valor
    return "unknown"


def _extrair_calendario(ds: xr.Dataset) -> str:
    """Lê o nome do calendário do eixo ``time``, normalizado em minúsculas.

    Prioriza ``encoding['calendar']`` (onde o xarray persiste o valor decodi-
    ficado); cai para ``attrs['calendar']`` e, em último caso, retorna
    ``"standard"``.
    """
    tempo = ds["time"]
    calendario = tempo.encoding.get("calendar") or tempo.attrs.get("calendar") or "standard"
    return str(calendario).lower()


class LeitorXarray:
    """Adaptador de leitura NetCDF baseado em ``xarray``.

    Implementa a porta :class:`~climate_risk.domain.portas.leitor_netcdf.LeitorNetCDF`
    encapsulando todas as particularidades do ecossistema NetCDF/``xarray``:
    múltiplas engines, fallback por cópia temporária, suporte a ``cftime``,
    identificação heurística de lat/lon e conversão de unidade.

    Args:
        copiar_para_tmp: Se ``True``, sempre copia o arquivo para um diretório
            temporário antes de abrir. Útil em storages read-only ou com
            locks agressivos. Padrão: ``False`` (tenta direto, cai para
            cópia apenas em ``PermissionError``).
    """

    def __init__(self, copiar_para_tmp: bool = False) -> None:
        self._copiar_para_tmp = copiar_para_tmp

    async def abrir(self, caminho: str, variavel: str) -> DadosClimaticos:
        """Lê e normaliza um arquivo NetCDF — ver :class:`LeitorNetCDF`."""
        if not Path(caminho).exists():
            raise ErroArquivoNCNaoEncontrado(
                caminho=caminho,
                detalhe="arquivo inexistente ou inacessível no filesystem.",
            )

        try:
            ds, tmpdir = await asyncio.to_thread(
                _abrir_com_fallback_copia,
                caminho,
                self._copiar_para_tmp,
            )
        except ErroLeituraNetCDF:
            raise
        except Exception as erro:
            raise ErroLeituraNetCDF(caminho=caminho, detalhe=str(erro)) from erro

        try:
            return await asyncio.to_thread(self._extrair, caminho, variavel, ds)
        finally:
            try:
                ds.close()
            finally:
                if tmpdir is not None:
                    shutil.rmtree(tmpdir, ignore_errors=True)

    def _extrair(self, caminho: str, variavel: str, ds: xr.Dataset) -> DadosClimaticos:
        """Núcleo síncrono da leitura — executado em thread separada.

        Todas as operações que tocam ``xarray``/``numpy`` ficam aqui para
        facilitar o envelope assíncrono em :meth:`abrir`.
        """
        try:
            if variavel not in ds.data_vars:
                raise ErroVariavelAusente(caminho=caminho, variavel=variavel)
            da = ds[variavel]
            if "time" not in da.dims:
                raise ErroDimensaoTempoAusente(caminho=caminho, variavel=variavel)

            # Conversão de unidade via domínio — NÃO reimplementar (ADR-007).
            resultado_conversao = ConversorPrecipitacao.para_mm_por_dia(da)
            da_mm = resultado_conversao.dados

            # Garantia de ordem: time deve ser a primeira dimensão; não
            # transpomos (respeito à ordem original da grade, ver brief).
            if da_mm.dims[0] != "time" or da_mm.ndim != 3:
                raise ErroLeituraNetCDF(
                    caminho=caminho,
                    detalhe=(
                        f"variável '{variavel}' com dims={da_mm.dims} (ndim={da_mm.ndim}); "
                        "esperado ndim=3 com 'time' como primeira dimensão."
                    ),
                )

            # Coordenadas lat/lon — reusa helpers do domínio (ADR-005).
            nome_lat, nome_lon = _identificar_coords_lat_lon(caminho, ds)
            lat_vals = np.asarray(ds[nome_lat].values)
            lon_vals = np.asarray(ds[nome_lon].values)
            lat_2d, lon_2d = coords_to_2d(lat_vals, lon_vals)
            lon_2d_neg180 = ensure_lon_negpos180(lon_2d)

            # Anos — ``.dt.year`` funciona com DatetimeIndex e CFTimeIndex.
            anos = np.asarray(da_mm["time"].dt.year.values, dtype=np.int64)

            dados_diarios = np.asarray(da_mm.values)
            calendario = _extrair_calendario(ds)
            cenario = _inferir_cenario(caminho, ds)

            return DadosClimaticos(
                dados_diarios=dados_diarios,
                lat_2d=np.asarray(lat_2d),
                lon_2d=np.asarray(lon_2d_neg180),
                anos=anos,
                cenario=cenario,
                variavel=variavel,
                unidade_original=resultado_conversao.unidade_original,
                conversao_unidade_aplicada=resultado_conversao.conversao_aplicada,
                calendario=calendario,
                arquivo_origem=caminho,
            )
        except ErroLeituraNetCDF:
            raise
        except Exception as erro:
            raise ErroLeituraNetCDF(caminho=caminho, detalhe=str(erro)) from erro
