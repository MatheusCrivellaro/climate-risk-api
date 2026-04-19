"""Entidade Município.

Representa um município brasileiro com dados do IBGE. Cache local alimentado
pelo caso de uso ``GeocodificarLocalizacoes`` (Slice 8).

Decisão (Slice 8): ``id`` mantido como ``int`` (código IBGE de 7 dígitos é
naturalmente inteiro) — preserva as *foreign keys* já existentes em
:class:`FornecedorORM` e :class:`ResultadoIndiceORM` do Slice 2. O brief
menciona ``str``; usamos ``int`` para evitar migração invasiva em FKs.

``lat_centroide``/``lon_centroide`` são opcionais para permitir inserir um
município vindo do catálogo IBGE (lista de localidades) antes de consultar
a malha — o *centroide* só é preenchido no segundo passo.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Municipio:
    """Município brasileiro identificado pelo código IBGE.

    Atributos:
        id: Código IBGE (7 dígitos) — identificador natural.
        nome: Nome oficial do município.
        nome_normalizado: Forma normalizada (sem acentos, minúsculo) usada para
            busca fuzzy — ver
            :func:`climate_risk.domain.util.normalizacao.normalizar_nome_municipio`.
        uf: Sigla da unidade federativa (ex.: ``"SP"``).
        lat_centroide: Latitude do centroide em graus decimais. ``None`` se a
            geometria da malha ainda não foi consultada.
        lon_centroide: Longitude do centroide em graus decimais. ``None`` se
            a geometria da malha ainda não foi consultada.
        atualizado_em: Momento da última sincronização com a API do IBGE.
    """

    id: int
    nome: str
    nome_normalizado: str
    uf: str
    lat_centroide: float | None
    lon_centroide: float | None
    atualizado_em: datetime
