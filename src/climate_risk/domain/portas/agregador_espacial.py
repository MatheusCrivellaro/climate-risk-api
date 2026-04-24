"""Porta :class:`AgregadorEspacial` — agregação de DataArrays por município.

Abstrai a operação de reduzir um ``xr.DataArray`` com dimensão temporal e
espacial a uma série temporal por município IBGE. O adaptador padrão é
:class:`~climate_risk.infrastructure.agregador_municipios_geopandas.AgregadorMunicipiosGeopandas`,
que faz *point-in-polygon* via ``geopandas`` e mantém um cache em disco
do mapeamento célula→município por grade.

Zero dependências de I/O nesta camada — apenas contrato tipado.
"""

from __future__ import annotations

from typing import Protocol

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
        """Agrega ``dados`` por município.

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
