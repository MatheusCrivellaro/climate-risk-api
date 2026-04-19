"""Caso de uso :class:`LocalizarPontos` (UC-04 — Slice 9).

Recebe pares ``(lat, lon)`` e devolve o município/UF que contém cada
ponto via consulta *point-in-polygon* na porta
:class:`ShapefileMunicipios`. Nenhuma I/O aqui — apenas orquestração.
"""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.domain.portas.shapefile_municipios import ShapefileMunicipios


@dataclass(frozen=True)
class PontoParaLocalizar:
    """Entrada do usuário — ``identificador`` é ecoado de volta."""

    lat: float
    lon: float
    identificador: str | None = None


@dataclass(frozen=True)
class PontoLocalizado:
    """Resposta unitária: dados do município se o ponto caiu em algum."""

    lat: float
    lon: float
    identificador: str | None
    encontrado: bool
    municipio_id: int | None
    uf: str | None
    nome_municipio: str | None


@dataclass(frozen=True)
class ResultadoLocalizacao:
    """Sumário agregado do lote."""

    total: int
    encontrados: int
    itens: list[PontoLocalizado]


class LocalizarPontos:
    """Orquestra a consulta vetorizada ao shapefile de municípios."""

    def __init__(self, shapefile: ShapefileMunicipios) -> None:
        self._shapefile = shapefile

    async def executar(self, pontos: list[PontoParaLocalizar]) -> ResultadoLocalizacao:
        if not pontos:
            return ResultadoLocalizacao(total=0, encontrados=0, itens=[])

        coords = [(p.lat, p.lon) for p in pontos]
        localizacoes = self._shapefile.localizar_pontos(coords)

        itens: list[PontoLocalizado] = []
        encontrados = 0
        for entrada, loc in zip(pontos, localizacoes, strict=True):
            if loc is None:
                itens.append(
                    PontoLocalizado(
                        lat=entrada.lat,
                        lon=entrada.lon,
                        identificador=entrada.identificador,
                        encontrado=False,
                        municipio_id=None,
                        uf=None,
                        nome_municipio=None,
                    )
                )
                continue
            encontrados += 1
            itens.append(
                PontoLocalizado(
                    lat=entrada.lat,
                    lon=entrada.lon,
                    identificador=entrada.identificador,
                    encontrado=True,
                    municipio_id=loc.municipio_id,
                    uf=loc.uf,
                    nome_municipio=loc.nome_municipio,
                )
            )
        return ResultadoLocalizacao(total=len(itens), encontrados=encontrados, itens=itens)
