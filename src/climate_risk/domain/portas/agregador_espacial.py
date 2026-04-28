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
    ) -> Iterator[tuple[int, np.ndarray, np.ndarray]]:
        """Yield ``(municipio_id, datas, serie_diaria)`` sob demanda.

        Variante streaming de :meth:`agregar_por_municipio`. Usada pelo
        pipeline de estresse hídrico (Slice 21) para evitar materialização
        do DataFrame completo em RAM. Memória por iteração é
        ``O(n_dias x n_celulas_do_municipio)``, não
        ``O(n_municipios x n_dias)``.

        Args:
            dados: mesmo contrato de :meth:`agregar_por_municipio`.

        Yields:
            Tuplas ``(municipio_id, datas, serie_diaria)``:

            - ``municipio_id``: código IBGE como ``int``.
            - ``datas``: ``np.ndarray`` 1D de timestamps (``datetime64``),
              um por dia da série.
            - ``serie_diaria``: ``np.ndarray`` 1D ``float64`` com a média
              espacial das células do município por dia. ``NaN`` quando
              todas as células do município estão mascaradas no dia.

        Determinismo:
            Múltiplas chamadas para a mesma grade produzem municípios
            **na mesma ordem**. Crítico para sincronização entre
            iteradores paralelos (pr/tas/evap) no pipeline.
        """
        ...
