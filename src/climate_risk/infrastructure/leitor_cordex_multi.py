"""Adaptador :class:`LeitorCordexMultiVariavel` — leitor multi-arquivo (Slice 13).

Implementa a porta :class:`~climate_risk.domain.portas.leitor_multivariavel.LeitorMultiVariavel`
lendo três arquivos CORDEX (``pr``, ``tas``, ``evspsbl``) e entregando uma
:class:`~climate_risk.domain.entidades.dados_multivariaveis.DadosClimaticosMultiVariaveis`
com eixo temporal comum (interseção) e unidades canônicas.

Diferente do :class:`~climate_risk.infrastructure.netcdf.leitor_xarray.LeitorXarray`:

- **Multi-variável por design**: recebe três caminhos, não identifica ``lat``/``lon`` 2D
  (grades preservadas; a agregação para município é passo posterior — ver ADR-009).
- **Síncrono**: o caso de uso que consome esta porta envelopa em ``asyncio.to_thread``
  se estiver em contexto ``async``. Mantemos sync aqui para simplificar o contrato.
- **Tolerante a calendários**: ``noleap`` e ``360_day`` são convertidos para gregoriano
  via ``xr.DataArray.convert_calendar`` — dias inexistentes viram ``NaN``.

Este módulo fica em ``infrastructure/`` (não em ``infrastructure/netcdf/``) porque
reusa ``_inferir_cenario`` e outros helpers do adapter uni-variável via import.
"""

from __future__ import annotations

import contextlib
import logging
import re
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from climate_risk.domain.entidades.dados_multivariaveis import DadosClimaticosMultiVariaveis
from climate_risk.domain.excecoes import (
    ErroArquivoNCNaoEncontrado,
    ErroCenarioInconsistente,
    ErroLeituraNetCDF,
    ErroPastaVazia,
    ErroVariavelAusente,
)

__all__ = [
    "LeitorCordexMultiVariavel",
    "_converter_unidade_precipitacao",
    "_converter_unidade_temperatura",
    "_identificar_variavel_principal",
    "_inferir_cenario_arquivo",
    "_normalizar_calendario",
]

logger = logging.getLogger(__name__)

_CALENDARIOS_NAO_GREGORIANOS = frozenset({"noleap", "365_day", "360_day", "all_leap", "366_day"})
_VARS_PRINCIPAIS: tuple[str, ...] = ("pr", "tas", "evspsbl")
_VARS_AUXILIARES: frozenset[str] = frozenset(
    {"time_bnds", "time_bounds", "rotated_pole", "height", "lat_bnds", "lon_bnds"}
)
_SCENARIO_RE = re.compile(r"(rcp\d{2}|ssp\d{3})", re.IGNORECASE)
_UNIDADES_FLUXO_PRECIPITACAO: tuple[str, ...] = (
    "kg m-2 s-1",
    "kg m^-2 s^-1",
    "mm s-1",
    "mm/s",
)


def _identificar_variavel_principal(caminho: str, ds: xr.Dataset) -> str:
    """Encontra o nome canônico (``pr``/``tas``/``evspsbl``) no dataset.

    Varre ``ds.data_vars`` ignorando auxiliares conhecidos (``time_bnds``,
    ``rotated_pole``, ``height``). Retorna o primeiro nome reconhecido.

    Raises:
        ErroVariavelAusente: quando nenhuma variável principal é encontrada.
    """
    for nome in ds.data_vars:
        if nome in _VARS_AUXILIARES:
            continue
        if nome in _VARS_PRINCIPAIS:
            return str(nome)
    disponiveis = ", ".join(str(v) for v in ds.data_vars) or "(vazio)"
    raise ErroVariavelAusente(
        caminho=caminho,
        variavel=(
            f"nenhuma variável principal ({'/'.join(_VARS_PRINCIPAIS)}) "
            f"encontrada — data_vars={disponiveis}."
        ),
    )


def _converter_unidade_precipitacao(da: xr.DataArray) -> xr.DataArray:
    """Converte ``pr`` ou ``evspsbl`` para ``mm/dia``.

    A heurística de precipitação (``ConversorPrecipitacao``) inclui um
    ``vmax < 5.0`` legado que não é adequado para evaporação
    (``evspsbl`` tem vmax intrinsecamente baixo mesmo já em mm/dia). Aqui
    usamos apenas a regra de unidade explícita — se ``units`` indica fluxo
    (kg m-2 s-1 ou equivalente), multiplicamos por 86400; caso contrário,
    assumimos que já está em mm/dia e apenas atualizamos ``attrs['units']``.
    """
    unidade_original = str(da.attrs.get("units", "") or "").lower()
    if any(ind in unidade_original for ind in _UNIDADES_FLUXO_PRECIPITACAO):
        da = da * 86400.0
    da.attrs["units"] = "mm/day"
    return da


def _converter_unidade_temperatura(da: xr.DataArray) -> xr.DataArray:
    """Converte ``tas`` para ``°C``.

    Se ``units`` começa com ``K`` (ou está vazio — default CORDEX é Kelvin),
    subtrai 273.15. Se já está em ``°C`` (``units = 'degC'`` / ``'celsius'``),
    mantém. Outras unidades (Fahrenheit, etc.) não são esperadas nos dados
    CORDEX e resultariam em ``ErroLeituraNetCDF`` levantado em camada acima
    quando os valores caírem fora da faixa plausível — não tratamos aqui.
    """
    unidade_original = str(da.attrs.get("units", "") or "").strip().lower()
    ja_em_celsius = unidade_original in {"degc", "celsius", "°c", "c"}
    if not ja_em_celsius:
        # Default: assume Kelvin (CORDEX convention). Vazio também cai aqui.
        da = da - 273.15
    da.attrs["units"] = "degC"
    return da


def _normalizar_calendario(da: xr.DataArray) -> xr.DataArray:
    """Garante eixo temporal em calendário gregoriano padrão.

    Arquivos com calendário ``noleap``/``360_day``/``all_leap`` são
    convertidos via ``convert_calendar("standard", use_cftime=False,
    align_on="date")`` — dias ausentes no calendário original
    (29/02 em ``noleap``; qualquer ``31`` em ``360_day``) viram ``NaN``,
    comportamento aceitável porque os cálculos de estresse hídrico
    descartam dias com ``NaN`` em qualquer variável.
    """
    calendario = _calendario_do_dataarray(da)
    if calendario not in _CALENDARIOS_NAO_GREGORIANOS:
        # Força conversão para DatetimeIndex se ainda for CFTimeIndex em standard.
        if not isinstance(da.indexes.get("time"), pd.DatetimeIndex):
            da = da.convert_calendar("standard", use_cftime=False, align_on="date")
        return da
    return da.convert_calendar("standard", use_cftime=False, align_on="date")


def _calendario_do_dataarray(da: xr.DataArray) -> str:
    """Lê o nome do calendário a partir de ``encoding``/``attrs`` do eixo ``time``."""
    if "time" not in da.coords:
        return "standard"
    coord = da["time"]
    calendario = coord.encoding.get("calendar") or coord.attrs.get("calendar") or "standard"
    return str(calendario).lower()


def _inferir_cenario_arquivo(caminho: str, ds: xr.Dataset) -> str:
    """Prioriza ``experiment_id``; cai em regex no nome do arquivo.

    A ordem aqui é **oposta** à do adapter uni-variável (que prioriza o
    regex do nome) — o brief desta slice pede explicitamente ``experiment_id``
    primeiro. Isso dá melhor aderência aos metadados oficiais CORDEX e
    evita falsos positivos em nomes que contenham ``rcp45`` em outras
    seções do path.
    """
    for chave in ("experiment_id", "scenario", "experiment"):
        valor = str(ds.attrs.get(chave, "")).strip().lower()
        if valor:
            return valor
    match = _SCENARIO_RE.search(Path(caminho).name)
    if match:
        return match.group(1).lower()
    return "unknown"


class LeitorCordexMultiVariavel:
    """Adaptador de leitura multi-variável para CORDEX.

    Implementa :class:`~climate_risk.domain.portas.leitor_multivariavel.LeitorMultiVariavel`.
    Não recebe configuração — a identificação da variável principal é feita
    via inspeção do dataset. Operações síncronas: o consumidor `async` deve
    envelopar com :func:`asyncio.to_thread` quando chamar de dentro de uma
    rota FastAPI.
    """

    def abrir(
        self,
        caminho_pr: Path,
        caminho_tas: Path,
        caminho_evap: Path,
    ) -> DadosClimaticosMultiVariaveis:
        """Lê os três arquivos e devolve o lote alinhado temporalmente."""
        caminhos_rotulados: tuple[tuple[str, Path], ...] = (
            ("precipitacao", caminho_pr),
            ("temperatura", caminho_tas),
            ("evaporacao", caminho_evap),
        )
        for rotulo, caminho in caminhos_rotulados:
            if not caminho.exists():
                raise ErroArquivoNCNaoEncontrado(
                    caminho=str(caminho),
                    detalhe=f"arquivo de {rotulo} inexistente ou inacessível.",
                )

        ds_pr = self._abrir_dataset(caminho_pr)
        ds_tas = self._abrir_dataset(caminho_tas)
        ds_evap = self._abrir_dataset(caminho_evap)

        try:
            da_pr = self._extrair_e_padronizar(ds_pr, caminho_pr, esperada="pr")
            da_tas = self._extrair_e_padronizar(ds_tas, caminho_tas, esperada="tas")
            da_evap = self._extrair_e_padronizar(ds_evap, caminho_evap, esperada="evspsbl")

            cenario = self._validar_cenarios_iguais(
                {
                    str(caminho_pr): _inferir_cenario_arquivo(str(caminho_pr), ds_pr),
                    str(caminho_tas): _inferir_cenario_arquivo(str(caminho_tas), ds_tas),
                    str(caminho_evap): _inferir_cenario_arquivo(str(caminho_evap), ds_evap),
                }
            )

            tempo_comum = self._intersectar_tempo(da_pr, da_tas, da_evap)
            if len(tempo_comum) == 0:
                raise ErroLeituraNetCDF(
                    caminho=str(caminho_pr),
                    detalhe="interseção temporal vazia entre os três arquivos.",
                )

            da_pr = da_pr.sel(time=tempo_comum).load()
            da_tas = da_tas.sel(time=tempo_comum).load()
            da_evap = da_evap.sel(time=tempo_comum).load()

            entidade = DadosClimaticosMultiVariaveis(
                precipitacao_diaria_mm=da_pr,
                temperatura_diaria_c=da_tas,
                evaporacao_diaria_mm=da_evap,
                tempo=tempo_comum,
                cenario=cenario,
            )
            entidade.validar()
            return entidade
        finally:
            for ds in (ds_pr, ds_tas, ds_evap):
                with contextlib.suppress(Exception):
                    ds.close()

    def abrir_de_pastas(
        self,
        pasta_pr: Path,
        pasta_tas: Path,
        pasta_evap: Path,
        cenario_esperado: str,
    ) -> DadosClimaticosMultiVariaveis:
        """Lê todos os ``.nc`` das três pastas, concatena no tempo e valida cenário.

        Cada pasta deve conter um ou mais arquivos NetCDF da mesma variável
        principal (``pr``/``tas``/``evspsbl``) e do mesmo cenário CORDEX. Os
        arquivos são abertos individualmente, validados quanto ao cenário
        declarado (``cenario_esperado``) e concatenados no eixo ``time``.

        Args:
            pasta_pr: Diretório com arquivos de precipitação.
            pasta_tas: Diretório com arquivos de temperatura do ar.
            pasta_evap: Diretório com arquivos de evaporação.
            cenario_esperado: Rótulo do cenário (``"rcp45"``/``"rcp85"``/...).
                Cada arquivo deve declarar (via ``experiment_id`` ou nome)
                este mesmo cenário, em case-insensitive.

        Raises:
            ErroPastaVazia: alguma pasta não contém ``.nc``.
            ErroCenarioInconsistente: algum arquivo declara cenário diferente.
            ErroLeituraNetCDF: interseção temporal entre as 3 variáveis vazia.
        """
        cenario_normalizado = cenario_esperado.strip().lower()
        rotulos: tuple[tuple[str, str, Path], ...] = (
            ("pr", "precipitacao", pasta_pr),
            ("tas", "temperatura", pasta_tas),
            ("evspsbl", "evaporacao", pasta_evap),
        )
        datasets_abertos: list[xr.Dataset] = []
        try:
            arrays_por_var: dict[str, xr.DataArray] = {}
            for var_canonica, rotulo, pasta in rotulos:
                arquivos = self._listar_nc_ordenados(pasta, var_canonica)
                arrays_arquivo: list[xr.DataArray] = []
                cenarios_divergentes: dict[str, str] = {}
                for arquivo in arquivos:
                    ds = self._abrir_dataset(arquivo)
                    datasets_abertos.append(ds)
                    cenario_arq = _inferir_cenario_arquivo(str(arquivo), ds)
                    if cenario_arq != "unknown" and cenario_arq != cenario_normalizado:
                        cenarios_divergentes[str(arquivo)] = cenario_arq
                    da = self._extrair_e_padronizar(ds, arquivo, esperada=var_canonica)
                    arrays_arquivo.append(da)
                if cenarios_divergentes:
                    cenarios_divergentes[f"<pasta {rotulo}>"] = cenario_normalizado
                    raise ErroCenarioInconsistente(cenarios_divergentes)
                arrays_por_var[var_canonica] = self._concatenar_no_tempo(
                    arrays_arquivo, var_canonica
                )

            da_pr = arrays_por_var["pr"]
            da_tas = arrays_por_var["tas"]
            da_evap = arrays_por_var["evspsbl"]

            tempo_comum = self._intersectar_tempo(da_pr, da_tas, da_evap)
            if len(tempo_comum) == 0:
                raise ErroLeituraNetCDF(
                    caminho=str(pasta_pr),
                    detalhe=(
                        "interseção temporal vazia entre pr/tas/evap "
                        f"(ranges: pr=[{_range_str(da_pr)}], "
                        f"tas=[{_range_str(da_tas)}], evap=[{_range_str(da_evap)}])."
                    ),
                )

            da_pr = da_pr.sel(time=tempo_comum).load()
            da_tas = da_tas.sel(time=tempo_comum).load()
            da_evap = da_evap.sel(time=tempo_comum).load()

            entidade = DadosClimaticosMultiVariaveis(
                precipitacao_diaria_mm=da_pr,
                temperatura_diaria_c=da_tas,
                evaporacao_diaria_mm=da_evap,
                tempo=tempo_comum,
                cenario=cenario_normalizado,
            )
            entidade.validar()
            return entidade
        finally:
            for ds in datasets_abertos:
                with contextlib.suppress(Exception):
                    ds.close()

    @staticmethod
    def _listar_nc_ordenados(pasta: Path, variavel: str) -> list[Path]:
        if not pasta.exists() or not pasta.is_dir():
            raise ErroPastaVazia(caminho=pasta, variavel=variavel)
        arquivos = sorted(pasta.glob("*.nc"))
        if not arquivos:
            raise ErroPastaVazia(caminho=pasta, variavel=variavel)
        return arquivos

    def _concatenar_no_tempo(self, arrays: list[xr.DataArray], variavel: str) -> xr.DataArray:
        """Concatena ``DataArray`` no eixo ``time``, ordena e remove duplicatas.

        Sobreposição (timestamps repetidos em arquivos diferentes) é resolvida
        mantendo o **primeiro** registro encontrado e logando warning. Mantemos
        a heurística simples — não tentamos detectar conflitos de valor.
        """
        if len(arrays) == 1:
            return arrays[0]
        concatenado = xr.concat(arrays, dim="time")
        tempos = pd.DatetimeIndex(concatenado["time"].values)
        if not tempos.is_monotonic_increasing:
            concatenado = concatenado.sortby("time")
            tempos = pd.DatetimeIndex(concatenado["time"].values)
        duplicados = tempos.duplicated(keep="first")
        if bool(duplicados.any()):
            n_dup = int(duplicados.sum())
            logger.warning(
                "Concatenação de %s: %d timestamps duplicados; mantendo o primeiro.",
                variavel,
                n_dup,
            )
            concatenado = concatenado.isel(time=~duplicados)
        return concatenado

    def _abrir_dataset(self, caminho: Path) -> xr.Dataset:
        """Abre o ``.nc`` com ``use_cftime=True`` para suportar calendários exóticos."""
        kwargs: dict[str, Any] = {
            "decode_times": True,
            "use_cftime": True,
            "mask_and_scale": True,
        }
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                return xr.open_dataset(caminho, **kwargs)
        except FileNotFoundError as erro:
            raise ErroArquivoNCNaoEncontrado(caminho=str(caminho), detalhe=str(erro)) from erro
        except Exception as erro:
            raise ErroLeituraNetCDF(caminho=str(caminho), detalhe=str(erro)) from erro

    def _extrair_e_padronizar(self, ds: xr.Dataset, caminho: Path, esperada: str) -> xr.DataArray:
        """Encontra a variável principal, converte unidade e normaliza calendário.

        ``esperada`` serve apenas para log: se a variável encontrada não for a
        esperada (ex.: alguém passou o arquivo de ``tas`` no slot de ``pr``),
        prosseguimos mesmo assim — a validação de cenário e o cálculo falharão
        com mensagens mais claras downstream. Preferimos não bloquear aqui
        para dar flexibilidade a experimentos futuros com outras variáveis.
        """
        nome = _identificar_variavel_principal(str(caminho), ds)
        if nome != esperada:
            logger.warning(
                "Arquivo %s tinha variável '%s' no slot esperado '%s'; seguindo mesmo assim.",
                caminho.name,
                nome,
                esperada,
            )
        da = ds[nome]
        if "time" not in da.dims:
            raise ErroLeituraNetCDF(
                caminho=str(caminho),
                detalhe=f"variável '{nome}' não possui dimensão 'time'.",
            )

        if nome in ("pr", "evspsbl"):
            da = _converter_unidade_precipitacao(da)
        elif nome == "tas":
            da = _converter_unidade_temperatura(da)
        da = _normalizar_calendario(da)
        return da

    def _intersectar_tempo(self, *arrays: xr.DataArray) -> pd.DatetimeIndex:
        """Devolve a interseção dos eixos ``time`` dos ``DataArray`` recebidos.

        Assume que cada eixo já é ``DatetimeIndex`` (pós :func:`_normalizar_calendario`).
        Usa ``reduce`` sobre ``DatetimeIndex.intersection`` preservando ordem ascendente.
        """
        idx = pd.DatetimeIndex(arrays[0]["time"].values)
        for da in arrays[1:]:
            idx = idx.intersection(pd.DatetimeIndex(da["time"].values))
        return idx.sort_values()

    def _validar_cenarios_iguais(self, cenarios_por_caminho: dict[str, str]) -> str:
        """Garante que os três arquivos reportam o mesmo cenário.

        Se todos forem ``"unknown"``, retorna ``"unknown"`` — não é erro, e
        deixa o caso de uso decidir se quer rejeitar. Se qualquer dois valores
        conhecidos divergirem, levanta :class:`ErroCenarioInconsistente`.
        """
        valores = set(cenarios_por_caminho.values())
        if len(valores) == 1:
            return next(iter(valores))
        raise ErroCenarioInconsistente(cenarios_por_caminho)


def _range_str(da: xr.DataArray) -> str:
    if "time" not in da.dims or da.sizes.get("time", 0) == 0:
        return "vazio"
    tempos = pd.DatetimeIndex(da["time"].values)
    return f"{tempos.min().date()}..{tempos.max().date()}"
