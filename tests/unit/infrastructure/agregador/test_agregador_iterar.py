"""Testes do streaming :meth:`AgregadorMunicipiosGeopandas.iterar_por_municipio`.

Slice 21 / ADR-013. Reusa as fixtures sintéticas de ``test_agregador_municipios``
(``mun_sintetico.shp`` + grades pré-preenchidas).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_risk.infrastructure.agregador_municipios_geopandas import (
    AgregadorMunicipiosGeopandas,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "agregador"
SHAPEFILE = FIXTURES / "mun_sintetico.shp"
GRADE_1D = FIXTURES / "grade_regular_1d.nc"

pytestmark = pytest.mark.skipif(
    not SHAPEFILE.exists(),
    reason="Fixtures sintéticas ausentes — rode tests/fixtures/agregador/gerar_fixtures.py.",
)


def test_iterar_retorna_iterator_e_nao_dataframe(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        resultado = agreg.iterar_por_municipio(da)
        assert isinstance(resultado, Iterator)
    finally:
        da.close()


def test_iterar_yield_uma_tupla_por_municipio(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = list(agreg.iterar_por_municipio(da))
    finally:
        da.close()

    assert len(tuplas) == 2
    municipios = {mun for mun, _, _ in tuplas}
    assert municipios == {9999999, 8888888}


def test_iterar_tipos_e_dimensoes_corretos(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = list(agreg.iterar_por_municipio(da))
    finally:
        da.close()

    for municipio_id, datas, serie in tuplas:
        assert isinstance(municipio_id, int)
        assert isinstance(datas, np.ndarray)
        assert isinstance(serie, np.ndarray)
        assert datas.ndim == 1
        assert serie.ndim == 1
        assert len(datas) == len(serie)
        # Grade 1D tem 3 timestamps.
        assert len(datas) == 3


def test_iterar_valores_corretos_media_espacial(tmp_path: Path) -> None:
    """Município A (oeste) tem células com valor 1.0; B (leste) tem 2.0."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        tuplas = dict((mun, serie) for mun, _, serie in agreg.iterar_por_municipio(da))
    finally:
        da.close()

    np.testing.assert_array_equal(tuplas[9999999], np.array([1.0, 1.0, 1.0]))
    np.testing.assert_array_equal(tuplas[8888888], np.array([2.0, 2.0, 2.0]))


def test_iterar_ordem_deterministica(tmp_path: Path) -> None:
    """Duas chamadas → mesma ordem de municípios. Crítico para sync no handler."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        ordem_1 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
        ordem_2 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
        ordem_3 = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
    finally:
        da.close()

    assert ordem_1 == ordem_2 == ordem_3
    # Ordem específica: ASC numérica (8888888 < 9999999).
    assert ordem_1 == [8888888, 9999999]


def test_iterar_grade_sem_municipios_nao_yield(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    # Cobre [20,22]° lat x [-60,-58]° lon — fora do shapefile sintético.
    lat = np.array([20.0, 22.0])
    lon = np.array([-60.0, -58.0])
    da = xr.DataArray(
        np.ones((2, 2, 2), dtype=np.float64),
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    tuplas = list(agreg.iterar_por_municipio(da))
    assert tuplas == []


def test_iterar_serie_nan_preservada(tmp_path: Path) -> None:
    """Quando todas as células de um município estão NaN num dia, ``serie[t]``
    é NaN (a média 'skipna' de zero valores válidos é NaN)."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
    lon = np.array([-49.0, -44.5, -40.0 + 1e-6, -35.5, -31.0])
    valores = np.full((2, 5, 5), 2.0, dtype=np.float64)
    valores[:, :, 0:2] = np.nan  # Município A inteiro NaN.
    da = xr.DataArray(
        valores, dims=("time", "lat", "lon"), coords={"time": tempo, "lat": lat, "lon": lon}
    )

    series = dict((mun, serie) for mun, _, serie in agreg.iterar_por_municipio(da))
    assert np.isnan(series[9999999]).all()  # A: NaN puro.
    np.testing.assert_array_equal(series[8888888], np.array([2.0, 2.0]))  # B: ok.


def test_legado_agregar_continua_devolvendo_dataframe(tmp_path: Path) -> None:
    """Regressão: o método legacy permanece funcionando para os callers antigos."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        df = agreg.agregar_por_municipio(da, nome_variavel="pr")
    finally:
        da.close()

    assert list(df.columns) == ["municipio_id", "data", "valor", "nome_variavel"]
    # IDs continuam strings (vêm do shapefile como string).
    assert set(df["municipio_id"].unique()) == {"9999999", "8888888"}
    assert len(df) == 6


# ---------------------------------------------------------------------
# Slice 22: municipios_mapeados + serie_de_municipio
# ---------------------------------------------------------------------


def test_municipios_mapeados_retorna_set_de_int(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        municipios = agreg.municipios_mapeados(da)
    finally:
        da.close()

    assert isinstance(municipios, set)
    assert municipios
    assert all(isinstance(m, int) for m in municipios)


def test_municipios_mapeados_consistente_com_iterar(tmp_path: Path) -> None:
    """O conjunto retornado bate exatamente com os IDs yielded por iterar."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        municipios_set = agreg.municipios_mapeados(da)
        municipios_iter = {mun for mun, _, _ in agreg.iterar_por_municipio(da)}
    finally:
        da.close()

    assert municipios_set == municipios_iter


def test_municipios_mapeados_usa_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Duas chamadas consecutivas devem reusar o mapa em memória."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        chamadas: list[int] = []
        original = agreg._obter_mapa_celulas

        def _spy(lat2d: np.ndarray, lon2d: np.ndarray) -> pd.DataFrame:
            chamadas.append(1)
            return original(lat2d, lon2d)

        monkeypatch.setattr(agreg, "_obter_mapa_celulas", _spy)

        agreg.municipios_mapeados(da)
        agreg.municipios_mapeados(da)
    finally:
        da.close()

    assert sum(chamadas) == 1


def test_municipios_mapeados_grade_sem_municipios_retorna_set_vazio(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    tempo = pd.date_range("2030-01-01", periods=2, freq="D")
    lat = np.array([20.0, 22.0])
    lon = np.array([-60.0, -58.0])
    da = xr.DataArray(
        np.ones((2, 2, 2), dtype=np.float64),
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
    )

    assert agreg.municipios_mapeados(da) == set()


def test_serie_de_municipio_retorna_datas_e_valores(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        datas, serie = agreg.serie_de_municipio(da, 9999999)
    finally:
        da.close()

    assert isinstance(datas, np.ndarray)
    assert isinstance(serie, np.ndarray)
    assert datas.ndim == 1 and serie.ndim == 1
    assert len(datas) == len(serie) == 3
    np.testing.assert_array_equal(serie, np.array([1.0, 1.0, 1.0]))


def test_serie_de_municipio_levanta_keyerror_para_municipio_ausente(tmp_path: Path) -> None:
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        with pytest.raises(KeyError, match="1234567"):
            agreg.serie_de_municipio(da, 1234567)
    finally:
        da.close()


def test_serie_de_municipio_reusa_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`municipios_mapeados` + `serie_de_municipio` na mesma grade só lê o
    mapa célula→município uma única vez (em memória)."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        chamadas: list[int] = []
        original = agreg._obter_mapa_celulas

        def _spy(lat2d: np.ndarray, lon2d: np.ndarray) -> pd.DataFrame:
            chamadas.append(1)
            return original(lat2d, lon2d)

        monkeypatch.setattr(agreg, "_obter_mapa_celulas", _spy)

        municipios = sorted(agreg.municipios_mapeados(da))
        for mun in municipios:
            agreg.serie_de_municipio(da, mun)
    finally:
        da.close()

    assert sum(chamadas) == 1


# ---------------------------------------------------------------------
# Slice 23: iterar_por_municipio com filtro municipios_alvo
# ---------------------------------------------------------------------


def test_iterar_por_municipio_sem_filtro_yield_todos(tmp_path: Path) -> None:
    """Backward-compat: chamada sem ``municipios_alvo`` igual à da Slice 21."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        ids_iterados = [mun for mun, _, _ in agreg.iterar_por_municipio(da)]
        # Equivalente ao caminho explícito None.
        ids_none = [mun for mun, _, _ in agreg.iterar_por_municipio(da, municipios_alvo=None)]
    finally:
        da.close()

    assert ids_iterados == ids_none == [8888888, 9999999]


def test_iterar_por_municipio_com_filtro_yield_apenas_intersecao(tmp_path: Path) -> None:
    """Filtro inclui IDs fora do mapa: iteração entrega só interseção."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        # Mapa da grade tem {8888888, 9999999}; alvo inclui 8888888 e dois
        # IDs fictícios fora da grade.
        alvo = {8888888, 1234567, 9999998}
        ids = [mun for mun, _, _ in agreg.iterar_por_municipio(da, municipios_alvo=alvo)]
    finally:
        da.close()

    assert ids == [8888888]


def test_iterar_por_municipio_com_filtro_vazio_yield_nada(tmp_path: Path) -> None:
    """Filtro vazio nunca produz municípios, mesmo com mapa não-vazio."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        ids = list(agreg.iterar_por_municipio(da, municipios_alvo=set()))
    finally:
        da.close()

    assert ids == []


def test_iterar_por_municipio_filtro_ordem_determinista(tmp_path: Path) -> None:
    """Mesma chamada filtrada 3x → mesma ordem ascendente. Precondição p/ ``zip``."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    alvo = {9999999, 8888888}
    try:
        ordem_1 = [mun for mun, _, _ in agreg.iterar_por_municipio(da, municipios_alvo=alvo)]
        ordem_2 = [mun for mun, _, _ in agreg.iterar_por_municipio(da, municipios_alvo=alvo)]
        ordem_3 = [mun for mun, _, _ in agreg.iterar_por_municipio(da, municipios_alvo=alvo)]
    finally:
        da.close()

    assert ordem_1 == ordem_2 == ordem_3
    assert ordem_1 == [8888888, 9999999]


def test_iterar_por_municipio_filtro_preserva_dados(tmp_path: Path) -> None:
    """Os valores yielded para um município filtrado batem com a iteração total."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = xr.open_dataarray(GRADE_1D)
    try:
        completo = {mun: serie for mun, _, serie in agreg.iterar_por_municipio(da)}
        filtrado = {
            mun: serie
            for mun, _, serie in agreg.iterar_por_municipio(da, municipios_alvo={9999999})
        }
    finally:
        da.close()

    assert set(filtrado.keys()) == {9999999}
    np.testing.assert_array_equal(filtrado[9999999], completo[9999999])


def test_iterar_3_grades_diferentes_sincronizam_com_intersecao(tmp_path: Path) -> None:
    """Três grades com mapeamentos distintos, filtradas pela interseção, sincronizam via ``zip``."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)

    # Construímos 3 grades sintéticas com cobertura distinta. A grade
    # ``mun_sintetico.shp`` cobre ambos {8888888, 9999999}; para simular
    # divergência, restringimos manualmente o ``municipios_alvo`` por
    # variável (a iteração final deve usar a interseção).
    da = xr.open_dataarray(GRADE_1D)
    try:
        municipios_pr = {8888888, 9999999}
        municipios_tas = {8888888, 9999999}
        # Simula evap "perdendo" o 8888888.
        municipios_evap = {9999999}

        intersecao = municipios_pr & municipios_tas & municipios_evap
        assert intersecao == {9999999}

        iter_pr = agreg.iterar_por_municipio(da, municipios_alvo=intersecao)
        iter_tas = agreg.iterar_por_municipio(da, municipios_alvo=intersecao)
        iter_evap = agreg.iterar_por_municipio(da, municipios_alvo=intersecao)

        tuplas = list(zip(iter_pr, iter_tas, iter_evap, strict=True))
    finally:
        da.close()

    # Deve haver exatamente 1 step pareando o município 9999999 nas 3.
    assert len(tuplas) == 1
    (mun_pr, _, _), (mun_tas, _, _), (mun_evap, _, _) = tuplas[0]
    assert mun_pr == mun_tas == mun_evap == 9999999


# ---------------------------------------------------------------------
# Slice 25: materialização única em iterar_por_municipio
# ---------------------------------------------------------------------


class _SpyDataArray:
    """Wrapper que conta acessos a ``.values``, delegando o resto.

    Usado para verificar que o iterator chama ``dados.values`` exatamente
    uma vez por execução, mesmo iterando por N municípios. Acessos a
    ``.values`` em DataArrays internos (ex.: ``dados['time'].values``)
    não passam pelo spy e portanto não são contados.
    """

    def __init__(self, da: xr.DataArray) -> None:
        self._da = da
        self.values_calls = 0

    @property
    def values(self) -> np.ndarray:
        self.values_calls += 1
        return self._da.values

    def __getattr__(self, name: str) -> object:
        return getattr(self._da, name)

    def __getitem__(self, key: object) -> object:
        return self._da[key]


def _grade_sintetica(n_municipios: int = 5, n_dias: int = 4) -> xr.DataArray:
    """Cria grade pequena cobrindo os 2 municípios da fixture sintética.

    Os municípios A (oeste, ID 9999999) e B (leste, ID 8888888) ocupam
    ``[-50, -40]`` e ``[-40, -30]`` em longitude respectivamente. Este
    helper fabrica grades com mais células que a fixture default para
    permitir testes de spy sem custo de I/O extra.
    """
    del n_municipios  # Apenas 2 municípios na fixture sintética.
    lat = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
    lon = np.array([-49.0, -44.5, -40.0 + 1e-6, -35.5, -31.0])
    tempo = pd.date_range("2030-01-01", periods=n_dias, freq="D")
    valores = np.zeros((n_dias, 5, 5), dtype=np.float64)
    valores[:, :, 0:2] = 1.0
    valores[:, :, 2:5] = 2.0
    return xr.DataArray(
        valores,
        dims=("time", "lat", "lon"),
        coords={"time": tempo, "lat": lat, "lon": lon},
        name="pr",
    )


def test_iterar_por_municipio_chama_dados_values_uma_vez(tmp_path: Path) -> None:
    """A materialização única é o coração da Slice 25: 1 compute por execução."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = _grade_sintetica()
    spy = _SpyDataArray(da)

    tuplas = list(agreg.iterar_por_municipio(spy))  # type: ignore[arg-type]

    assert len(tuplas) == 2  # 2 municípios na fixture
    assert spy.values_calls == 1


def test_iterar_por_municipio_com_filtro_chama_values_uma_vez(tmp_path: Path) -> None:
    """Mesmo com filtro ``municipios_alvo``, a materialização permanece única."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = _grade_sintetica()
    spy = _SpyDataArray(da)

    tuplas = list(
        agreg.iterar_por_municipio(spy, municipios_alvo={9999999})  # type: ignore[arg-type]
    )

    assert len(tuplas) == 1
    assert spy.values_calls == 1


def test_iterar_por_municipio_filtro_vazio_nao_chama_values(tmp_path: Path) -> None:
    """Otimização: filtro vazio nunca paga o custo do compute completo."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = _grade_sintetica()
    spy = _SpyDataArray(da)

    tuplas = list(
        agreg.iterar_por_municipio(spy, municipios_alvo=set())  # type: ignore[arg-type]
    )

    assert tuplas == []
    assert spy.values_calls == 0


def test_iterar_por_municipio_resultado_identico_ao_legacy(tmp_path: Path) -> None:
    """Paridade numérica com ``_agregar_por_municipio_com_mapa`` (referência)."""
    agreg = AgregadorMunicipiosGeopandas(SHAPEFILE, tmp_path)
    da = _grade_sintetica(n_dias=5)

    do_iterator = {int(mun): serie for mun, _, serie in agreg.iterar_por_municipio(da)}
    df_legacy = agreg.agregar_por_municipio(da, nome_variavel="pr")
    do_legacy: dict[int, np.ndarray] = {}
    for mun_str, grupo in df_legacy.groupby("municipio_id"):
        do_legacy[int(str(mun_str))] = grupo["valor"].to_numpy(dtype=np.float64)

    assert set(do_iterator.keys()) == set(do_legacy.keys())
    for municipio_id, serie_iter in do_iterator.items():
        np.testing.assert_allclose(
            serie_iter,
            do_legacy[municipio_id],
            rtol=1e-9,
            atol=1e-12,
            err_msg=f"Divergência no município {municipio_id}",
        )
