"""Testes de :class:`ShapefileGeopandas` com fixture sintética.

Exige o fixture gerado por ``scripts/gerar_shapefile_fixture.py`` — se
ausente, os testes são skipados (marker ``shapefile`` + ``@pytest.mark``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from climate_risk.domain.excecoes import ErroConfiguracao
from climate_risk.infrastructure.shapefile import ShapefileGeopandas

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "shapefile" / "municipios_minimo.shp"


pytestmark = [
    pytest.mark.shapefile,
    pytest.mark.skipif(not FIXTURE.exists(), reason="Fixture shapefile ausente."),
]


def test_carrega_e_localiza_sao_paulo() -> None:
    sf = ShapefileGeopandas(str(FIXTURE))
    loc = sf.localizar_ponto(lat=-23.55, lon=-46.63)
    assert loc is not None
    assert loc.municipio_id == 3550308
    assert loc.uf == "SP"
    assert loc.nome_municipio == "São Paulo"


def test_ponto_fora_de_qualquer_municipio() -> None:
    sf = ShapefileGeopandas(str(FIXTURE))
    assert sf.localizar_ponto(lat=0.0, lon=0.0) is None


def test_lote_vetorizado() -> None:
    sf = ShapefileGeopandas(str(FIXTURE))
    resultado = sf.localizar_pontos(
        [
            (-23.55, -46.63),  # São Paulo
            (0.0, 0.0),  # Atlântico
            (-22.90, -43.20),  # Rio de Janeiro
        ]
    )
    assert resultado[0] is not None
    assert resultado[0].municipio_id == 3550308
    assert resultado[1] is None
    assert resultado[2] is not None
    assert resultado[2].uf == "RJ"


def test_lote_vazio() -> None:
    sf = ShapefileGeopandas(str(FIXTURE))
    assert sf.localizar_pontos([]) == []


def test_caminho_inexistente_levanta_erro_configuracao() -> None:
    with pytest.raises(ErroConfiguracao, match="não encontrado"):
        ShapefileGeopandas("/nao/existe.shp")


def test_caminho_vazio_levanta_erro_configuracao() -> None:
    with pytest.raises(ErroConfiguracao, match="não configurado"):
        ShapefileGeopandas("")
