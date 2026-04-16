"""Entidade Município.

Representa um município brasileiro com dados do IBGE. Cache local alimentado
pelo caso de uso ``GeocodificarLocalizacoes`` (Slice 8).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Municipio:
    """Município brasileiro identificado pelo código IBGE.

    Atributos:
        id: Código IBGE (7 dígitos) — identificador natural.
        nome: Nome oficial do município.
        nome_normalizado: Forma normalizada (sem acentos, minúsculo) usada para
            busca fuzzy no Slice 8.
        uf: Sigla da unidade federativa (ex.: ``"SP"``).
        lat_centroide: Latitude do centroide em graus decimais.
        lon_centroide: Longitude do centroide em graus decimais.
    """

    id: int
    nome: str
    nome_normalizado: str
    uf: str
    lat_centroide: float
    lon_centroide: float
