"""Porta :class:`ShapefileMunicipios` (UC-04 — Slice 9).

Abstrai a consulta *point-in-polygon* sobre a malha de municípios. O
adaptador padrão é :class:`ShapefileGeopandas` (infraestrutura), que
carrega um shapefile do IBGE. Para testes, basta implementar a mesma
interface em memória sem tocar ``geopandas``.

Todas as assinaturas são síncronas — a operação é puramente CPU-bound
e o adaptador mantém o índice espacial em memória.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class LocalizacaoGeografica:
    """Município devolvido por :meth:`ShapefileMunicipios.localizar_ponto`."""

    municipio_id: int
    uf: str
    nome_municipio: str


class ShapefileMunicipios(Protocol):
    """Consulta reversa (lat/lon → município) via shapefile do IBGE."""

    def localizar_ponto(self, lat: float, lon: float) -> LocalizacaoGeografica | None:
        """Retorna o município que contém o ponto, ou ``None`` se fora.

        ``None`` significa que o par (lat, lon) não caiu dentro de nenhum
        polígono — o ponto está fora do território brasileiro coberto pelo
        shapefile, em mar aberto ou em área de fronteira imprecisa.
        """
        ...

    def localizar_pontos(
        self, pontos: list[tuple[float, float]]
    ) -> list[LocalizacaoGeografica | None]:
        """Versão vetorizada — devolve a lista alinhada 1:1 com a entrada.

        Deve ser muito mais rápido que chamar :meth:`localizar_ponto` em
        loop, pois adaptadores como ``geopandas`` aproveitam o índice
        espacial para ``sjoin`` em bloco.
        """
        ...
