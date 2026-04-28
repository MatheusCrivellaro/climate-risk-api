"""Porta :class:`AgregadorEspacial` — agregação de DataArrays por município.

Abstrai a operação de reduzir um ``xr.DataArray`` com dimensão temporal e
espacial a uma série temporal por município IBGE. O adaptador padrão é
:class:`~climate_risk.infrastructure.agregador_municipios_geopandas.AgregadorMunicipiosGeopandas`,
que faz *point-in-polygon* via ``geopandas`` e mantém um cache em disco
do mapeamento célula→município por grade.

Zero dependências de I/O nesta camada — apenas contrato tipado.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

import numpy as np
import pandas as pd
import xarray as xr


class AgregadorEspacial(Protocol):
    """Porta para agregadores espaciais de DataArrays climáticos.

    Dado um DataArray com dimensão temporal e espacial, agrega para série
    temporal por município. Implementações típicas fazem média espacial
    (``skipna``) das células que caem dentro de cada polígono municipal.
    """

    def agregar_por_municipio(
        self,
        dados: xr.DataArray,
        nome_variavel: str,
    ) -> pd.DataFrame:
        """Agrega ``dados`` por município (formato eager, em DataFrame).

        Mantido por compatibilidade com chamadas legadas. Para datasets
        grandes (> alguns anos x milhares de municípios), prefira
        :meth:`iterar_por_municipio` — este método materializa o resultado
        completo em memória.

        Args:
            dados: DataArray com dimensão ``time`` e duas dimensões
                espaciais (``y``/``x`` ou ``rlat``/``rlon``). Deve expor
                coordenadas ``lat`` e ``lon`` (1D ou 2D) para que o
                adaptador possa localizar as células.
            nome_variavel: Rótulo da variável climática (ex.: ``"pr"``).
                Propagado para a coluna homônima do DataFrame de saída.

        Returns:
            DataFrame com colunas ``[municipio_id, data, valor, nome_variavel]``,
            uma linha por combinação (município, timestamp). Municípios sem
            células na grade são omitidos; células fora de qualquer município
            são descartadas.
        """
        ...

    def iterar_por_municipio(
        self,
        dados: xr.DataArray,
        *,
        municipios_alvo: set[int] | None = None,
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        """Yield ``(municipio_id, datas, serie_diaria)`` sob demanda.

        Variante streaming de :meth:`agregar_por_municipio`. Usada pelo
        pipeline de estresse hídrico (Slice 21) para evitar materialização
        do DataFrame completo em RAM. Memória por iteração é
        ``O(n_dias x n_celulas_do_municipio)``, não
        ``O(n_municipios x n_dias)``.

        Args:
            dados: mesmo contrato de :meth:`agregar_por_municipio`.
            municipios_alvo: se fornecido (Slice 23), itera apenas pelos
                municípios que estão **tanto** no mapeamento da grade
                **quanto** neste conjunto. Se ``None`` (default), itera
                por todos os municípios mapeados — comportamento da
                Slice 21.

        Yields:
            Tuplas ``(municipio_id, datas, serie_diaria)``:

            - ``municipio_id``: código IBGE como ``int``.
            - ``datas``: ``np.ndarray`` 1D de timestamps (``datetime64``),
              um por dia da série.
            - ``serie_diaria``: ``np.ndarray`` 1D ``float64`` com a média
              espacial das células do município por dia. ``NaN`` quando
              todas as células do município estão mascaradas no dia.

        Determinismo:
            Os IDs são yielded em **ordem ascendente** de ``municipio_id``.
            Múltiplas chamadas com o mesmo ``municipios_alvo`` produzem
            municípios na mesma ordem — precondição para sincronizar
            iteradores paralelos (pr/tas/evap) via ``zip`` quando o
            mesmo conjunto filtrado é passado às 3 chamadas.

        Uso recomendado para múltiplas variáveis (Slice 23):
            Calcule a interseção via :meth:`municipios_mapeados` para as
            3 variáveis, passe o mesmo conjunto como ``municipios_alvo``
            às 3 chamadas e consuma com ``zip``. Isso preserva a
            performance do streaming dask (1 ``compute`` por variável)
            sem dessincronizar.
        """
        ...

    def municipios_mapeados(self, dados: xr.DataArray) -> set[int]:
        """Retorna o conjunto de municípios mapeados na grade de ``dados``.

        Operação leve: não materializa séries diárias e reusa o cache do
        mapeamento célula→município se já tiver sido construído para a
        mesma grade. Usar para calcular interseção entre coberturas de
        variáveis distintas antes de iterar (Slice 22 / ADR-014).

        Args:
            dados: DataArray com coordenadas ``lat``/``lon`` (mesmo
                contrato de :meth:`agregar_por_municipio`).

        Returns:
            Conjunto de IDs IBGE como ``int``.
        """
        ...

    def serie_de_municipio(
        self,
        dados: xr.DataArray,
        municipio_id: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Retorna ``(datas, serie_diaria)`` para um único município.

        Variante ponto-a-ponto de :meth:`iterar_por_municipio`. Permite
        consumir municípios fora da ordem natural da grade — útil para
        debug, exportação ad-hoc ou análise pontual de um município
        específico.

        Args:
            dados: DataArray nos mesmos termos de
                :meth:`agregar_por_municipio`.
            municipio_id: código IBGE do município.

        Returns:
            Tupla ``(datas, serie_diaria)``:

            - ``datas``: ``np.ndarray`` 1D ``datetime64`` com timestamps.
            - ``serie_diaria``: ``np.ndarray`` 1D ``float64`` com a média
              espacial das células do município por dia.

        Raises:
            KeyError: se ``municipio_id`` não estiver mapeado nesta grade.

        Nota:
            Para processamento em massa de muitos municípios, **não**
            chamar este método em loop — cada chamada dispara um
            ``compute`` dask separado, perdendo a localidade do
            streaming. Use :meth:`iterar_por_municipio` com
            ``municipios_alvo`` (Slice 23 / ADR-015) que mantém a
            performance.
        """
        ...
