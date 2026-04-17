"""Porta :class:`LeitorNetCDF` — contrato de leitura de arquivos CORDEX.

Definida como ``typing.Protocol`` para permitir duck typing estrutural:
qualquer classe que exponha o método ``async abrir(caminho, variavel)``
com a assinatura declarada satisfaz o contrato (ver ADR-005).
"""

from __future__ import annotations

from typing import Protocol

from climate_risk.domain.entidades.dados_climaticos import DadosClimaticos


class LeitorNetCDF(Protocol):
    """Contrato para leitura de arquivos NetCDF climáticos.

    Implementações são responsáveis por:

    - Abrir o arquivo (tratando múltiplas engines quando necessário).
    - Identificar o nome da variável climática, de ``lat`` e de ``lon``.
    - Converter a unidade para ``mm/dia`` via
      :class:`~climate_risk.domain.unidades.conversores.ConversorPrecipitacao`.
    - Normalizar longitudes para ``[-180, 180]``.
    - Extrair anos dos timestamps, suportando calendários ``cftime``
      (ex.: ``360_day``, ``noleap``).
    - Inferir o cenário (``rcp`` / ``ssp``) do nome do arquivo ou atributos.
    - Fechar o arquivo e liberar recursos (inclusive pasta temporária,
      se houve cópia).

    **Não** são responsáveis por calcular índices, filtrar pontos ou
    persistir resultados — essas responsabilidades pertencem às camadas
    ``domain/indices`` e ``application``.
    """

    async def abrir(self, caminho: str, variavel: str) -> DadosClimaticos:
        """Lê e normaliza dados de um arquivo NetCDF.

        Args:
            caminho: caminho local para o arquivo ``.nc``.
            variavel: nome da variável climática (ex.: ``"pr"``).

        Returns:
            :class:`DadosClimaticos` com arrays em ``mm/dia`` e metadados
            normalizados.

        Raises:
            ErroArquivoNCNaoEncontrado: arquivo não existe ou não é acessível.
            ErroVariavelAusente: variável não está no dataset.
            ErroDimensaoTempoAusente: variável sem dimensão ``time``.
            ErroCoordenadasLatLonAusentes: não foi possível identificar
                ``lat``/``lon`` nas coords/variables.
            ErroLeituraNetCDF: outros erros de leitura (I/O, corrupção, etc.)
                traduzidos da biblioteca subjacente.
        """
        ...
