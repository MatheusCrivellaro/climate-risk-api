"""Entidade Fornecedor.

Representa um ponto de interesse (tipicamente um fornecedor industrial) cujas
coordenadas geográficas serão avaliadas contra índices climáticos.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Fornecedor:
    """Fornecedor geolocalizado.

    Atributos:
        id: ULID com prefixo ``"forn_"`` gerado pela aplicação.
        identificador_externo: Identificador opcional fornecido pelo cliente
            (ex.: código interno). Pode ser ``None``.
        nome: Nome de apresentação.
        lat: Latitude em graus decimais.
        lon: Longitude em graus decimais (convenção ``-180..180``).
        municipio_id: Código IBGE do município, quando já geocodificado.
        criado_em: Timestamp de criação em UTC.
    """

    id: str
    identificador_externo: str | None
    nome: str
    lat: float
    lon: float
    municipio_id: int | None
    criado_em: datetime
