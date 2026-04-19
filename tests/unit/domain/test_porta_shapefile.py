"""Contrato :class:`LocalizacaoGeografica` + :class:`ShapefileMunicipios`."""

from __future__ import annotations

import dataclasses

import pytest

from climate_risk.domain.portas.shapefile_municipios import (
    LocalizacaoGeografica,
    ShapefileMunicipios,
)


def test_localizacao_geografica_eh_frozen() -> None:
    loc = LocalizacaoGeografica(municipio_id=3550308, uf="SP", nome_municipio="São Paulo")
    assert dataclasses.is_dataclass(loc)
    with pytest.raises(dataclasses.FrozenInstanceError):
        loc.uf = "RJ"  # type: ignore[misc]


def test_localizacao_geografica_campos() -> None:
    loc = LocalizacaoGeografica(municipio_id=1, uf="MG", nome_municipio="X")
    assert loc.municipio_id == 1
    assert loc.uf == "MG"
    assert loc.nome_municipio == "X"


def test_shapefile_municipios_eh_protocol() -> None:
    class _Falso:
        def localizar_ponto(self, lat: float, lon: float) -> LocalizacaoGeografica | None:
            return None

        def localizar_pontos(
            self, pontos: list[tuple[float, float]]
        ) -> list[LocalizacaoGeografica | None]:
            return [None] * len(pontos)

    falso: ShapefileMunicipios = _Falso()
    assert falso.localizar_ponto(0.0, 0.0) is None
    assert falso.localizar_pontos([(0.0, 0.0), (1.0, 1.0)]) == [None, None]
