"""Normalização canônica de nomes de municípios brasileiros.

A função :func:`normalizar_nome_municipio` é a chave de comparação usada pelo
caso de uso ``GeocodificarLocalizacoes`` (Slice 8) tanto para *match* exato
quanto para o *fuzzy match* via ``rapidfuzz``. Implementada puramente em
``stdlib`` (``unicodedata`` + ``re``) — mantém ``domain/`` livre de
dependências externas (ADR-005).

Transformações aplicadas (na ordem):

1. ``strip`` e ``casefold`` (minúsculo robusto para unicode).
2. Substituição dos apóstrofos tipográficos (U+2018, U+2019, U+00B4, backtick)
   por ``'`` antes da regra ``d'oeste → doeste``.
3. Aplicação da regra portuguesa ``d'oeste → doeste`` (evita que o hífen ou
   o apóstrofo separem "oeste" do prefixo em municípios como
   "Alta Floresta D'Oeste").
4. Remoção de acentos via decomposição NFKD + filtro de ``combining chars``.
5. Hífens viram espaços (``"são joão del-rei" → "sao joao del rei"``).
6. Remoção de apóstrofos remanescentes.
7. Colapso de espaços múltiplos em um único espaço e ``strip`` final.

Exemplos:

- ``"São Paulo"         → "sao paulo"``
- ``"FLORIANÓPOLIS"     → "florianopolis"``
- ``"D'Ávila"           → "davila"`` (apóstrofo sobrevive só ao ``d'oeste``)
- ``"Alta Floresta D'Oeste" → "alta floresta doeste"``
- ``"São João del-Rei"  → "sao joao del rei"``
- ``"  Curitiba "       → "curitiba"``
"""

from __future__ import annotations

import re
import unicodedata

_APOSTROFOS_TIPOGRAFICOS = str.maketrans({"\u2018": "'", "\u2019": "'", "\u00b4": "'", "`": "'"})
_REGRA_D_OESTE = re.compile(r"d'oeste", re.IGNORECASE)
_ESPACOS = re.compile(r"\s+")


def normalizar_nome_municipio(nome: str) -> str:
    """Converte um nome de município em sua forma canônica para busca.

    Args:
        nome: Nome bruto como digitado pelo usuário ou retornado pela API do
            IBGE (pode conter acentos, maiúsculas, apóstrofos tipográficos,
            hífens e espaços múltiplos).

    Returns:
        Chave de comparação estável, sem acentos, sem apóstrofos, sem hífens
        e em minúsculo. Entrada vazia / ``None``-like vazio devolve ``""``.
    """
    if not nome:
        return ""

    texto = nome.strip().casefold()
    texto = texto.translate(_APOSTROFOS_TIPOGRAFICOS)
    texto = _REGRA_D_OESTE.sub("doeste", texto)

    # Decompõe em NFKD e descarta caracteres "combining" (acentos, tis, etc.).
    decomposto = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(ch for ch in decomposto if not unicodedata.combining(ch))

    sem_acento = sem_acento.replace("-", " ").replace("'", "")
    return _ESPACOS.sub(" ", sem_acento).strip()
