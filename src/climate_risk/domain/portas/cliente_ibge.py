"""Porta :class:`ClienteIBGE` — fronteira sobre a API de Localidades do IBGE.

O adaptador concreto vive em
``infrastructure/geocodificacao/cliente_ibge_http.py`` e encapsula os
endpoints:

- ``GET /api/v1/localidades/municipios`` — catálogo (id, nome, UF).
- ``GET /api/v3/malhas/municipios/{id}?formato=application/vnd.geo+json`` —
  geometria GeoJSON.

Manter esta porta em ``domain/portas/`` garante que ``application/`` não
dependa de ``httpx`` (ADR-005).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class MunicipioIBGE:
    """Registro bruto do catálogo IBGE (sem centroide).

    Atributos:
        id: Código IBGE de 7 dígitos.
        nome: Nome oficial como retornado pelo IBGE.
        uf: Sigla da UF (ex.: ``"SP"``).
    """

    id: int
    nome: str
    uf: str


class ClienteIBGE(Protocol):
    """Fronteira assíncrona sobre a API REST do IBGE."""

    async def listar_municipios(self) -> list[MunicipioIBGE]:
        """Lista todos os municípios brasileiros (≈5570 itens).

        Implementações devem mapear o JSON do IBGE para :class:`MunicipioIBGE`
        em um único passo — sem expor ``dict``/``Response`` para chamadas
        superiores.
        """
        ...

    async def obter_geometria_municipio(self, municipio_id: int) -> dict[str, Any]:
        """Retorna o GeoJSON bruto (``Feature`` ou ``FeatureCollection``).

        Args:
            municipio_id: Código IBGE de 7 dígitos.

        Returns:
            Documento GeoJSON tal qual veio da API — a extração do centroide
            é responsabilidade de :class:`CalculadorCentroide`.
        """
        ...
