"""Entidade Fornecedor.

Representa um ponto de interesse (tipicamente um fornecedor industrial) cujas
coordenadas geográficas serão avaliadas contra índices climáticos.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Fornecedor:
    """Fornecedor.

    Atributos:
        id: ULID com prefixo ``"forn_"`` gerado pela aplicação.
        nome: Nome da empresa / identificação de apresentação.
        cidade: Município declarado pelo cliente (pode divergir do nome
            canônico do IBGE).
        uf: Sigla da UF (2 letras maiúsculas).
        identificador_externo: Identificador opcional fornecido pelo cliente
            (ex.: código interno). Pode ser ``None``.
        lat: Latitude em graus decimais, preenchida após geocodificação.
        lon: Longitude em graus decimais (convenção ``-180..180``).
        municipio_id: Código IBGE do município, quando já geocodificado.
        criado_em: Timestamp de criação em UTC.
        atualizado_em: Timestamp da última modificação em UTC.
    """

    id: str
    nome: str
    cidade: str
    uf: str
    criado_em: datetime
    atualizado_em: datetime
    identificador_externo: str | None = None
    lat: float | None = None
    lon: float | None = None
    municipio_id: int | None = None
