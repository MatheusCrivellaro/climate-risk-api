"""Porta :class:`LeitorMultiVariavel` — contrato de leitura multi-arquivo.

Paralela a :class:`~climate_risk.domain.portas.leitor_netcdf.LeitorNetCDF`
(que é uni-variável). Os dois contratos coexistem e não se confundem — um
mesmo adapter pode implementar os dois se fizer sentido, mas nenhuma
dependência é assumida. Ver ADR-009 para a justificativa de não estender
``LeitorNetCDF``.

A porta declara apenas tipos; zero dependência de I/O, xarray ou outras
libs de ``infrastructure``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from climate_risk.domain.entidades.dados_multivariaveis import DadosClimaticosMultiVariaveis


class LeitorMultiVariavelPasta(Protocol):
    """Variante do :class:`LeitorMultiVariavel` que lê **pastas** (Slice 17).

    Cada pasta contém um ou mais arquivos ``.nc`` da mesma variável e
    cenário. A implementação concatena no eixo ``time``, valida que cada
    arquivo declara o ``cenario_esperado`` e devolve a entidade com as três
    séries alinhadas.
    """

    def abrir_de_pastas(
        self,
        pasta_pr: Path,
        pasta_tas: Path,
        pasta_evap: Path,
        cenario_esperado: str,
    ) -> DadosClimaticosMultiVariaveis:
        """Concatena ``.nc`` das três pastas e valida cenário declarado.

        Raises:
            ErroPastaVazia: alguma pasta não contém ``.nc``.
            ErroCenarioInconsistente: arquivo declara cenário diferente
                de ``cenario_esperado``.
            ErroLeituraNetCDF: interseção temporal entre pr/tas/evap vazia.
        """
        ...


class LeitorMultiVariavel(Protocol):
    """Contrato para leitura coordenada de três arquivos CORDEX.

    Implementações são responsáveis por:

    - Abrir cada arquivo e identificar a variável principal (``pr``,
      ``tas`` ou ``evspsbl``) ignorando auxiliares (``time_bnds``,
      ``rotated_pole``, ``height``).
    - Converter unidades para as grandezas canônicas do domínio
      (``mm/dia`` para ``pr``/``evspsbl``, ``°C`` para ``tas``).
    - Normalizar o calendário de cada série para gregoriano (``noleap``
      e ``360_day`` convertidos via ``convert_calendar``).
    - Validar que os três arquivos reportam o **mesmo cenário**
      (``experiment_id`` ou padrão no nome).
    - Tomar a interseção temporal e reindexar cada ``DataArray`` para esse
      eixo comum antes de entregar.
    - Fechar os datasets e liberar recursos.
    """

    def abrir(
        self,
        caminho_pr: Path,
        caminho_tas: Path,
        caminho_evap: Path,
    ) -> DadosClimaticosMultiVariaveis:
        """Lê os três arquivos e devolve o lote alinhado temporalmente.

        Args:
            caminho_pr: caminho local para o ``.nc`` de precipitação.
            caminho_tas: caminho local para o ``.nc`` de temperatura do ar.
            caminho_evap: caminho local para o ``.nc`` de evaporação.

        Returns:
            :class:`DadosClimaticosMultiVariaveis` com as três ``DataArray``
            em unidades canônicas e com o eixo temporal já interseccionado.

        Raises:
            ErroArquivoNCNaoEncontrado: qualquer um dos três arquivos não
                existe ou é inacessível.
            ErroLeituraNetCDF: erro genérico de leitura (parsing, engine).
            ErroVariavelAusente: nenhuma variável principal reconhecida no
                arquivo.
            ErroCenarioInconsistente: os três arquivos reportam cenários
                diferentes.
        """
        ...
