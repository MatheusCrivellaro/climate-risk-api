"""Testes unitários de :mod:`climate_risk.domain.util.distancia`."""

from __future__ import annotations

import math

import pytest

from climate_risk.domain.util.distancia import (
    bbox_aproximado_por_raio,
    distancia_haversine_km,
)


class TestDistanciaHaversine:
    """Casos conhecidos + simetria + extremos."""

    def test_ponto_identico_retorna_zero(self) -> None:
        assert distancia_haversine_km(0.0, 0.0, 0.0, 0.0) == 0.0

    def test_sp_rio_aproximadamente_358_km(self) -> None:
        # São Paulo (-23.55, -46.63) → Rio (-22.91, -43.21)
        distancia = distancia_haversine_km(-23.55, -46.63, -22.91, -43.21)
        assert 355.0 <= distancia <= 365.0

    def test_polo_norte_ao_equador_aproximadamente_10007_km(self) -> None:
        distancia = distancia_haversine_km(90.0, 0.0, 0.0, 0.0)
        esperado = math.pi / 2 * 6371.0
        assert distancia == pytest.approx(esperado, rel=1e-6)
        assert 10000.0 < distancia < 10015.0

    def test_antipoda_aproximadamente_meio_globo(self) -> None:
        distancia = distancia_haversine_km(0.0, 0.0, 0.0, 180.0)
        esperado = math.pi * 6371.0
        assert distancia == pytest.approx(esperado, rel=1e-6)

    def test_comutativo(self) -> None:
        d1 = distancia_haversine_km(10.0, 20.0, -5.0, 45.0)
        d2 = distancia_haversine_km(-5.0, 45.0, 10.0, 20.0)
        assert d1 == pytest.approx(d2, abs=1e-9)

    def test_deslocamento_longitudinal_no_equador(self) -> None:
        # 1 grau de longitude no equador ≈ 111.195 km.
        distancia = distancia_haversine_km(0.0, 0.0, 0.0, 1.0)
        assert 111.0 < distancia < 111.5

    def test_um_grau_de_latitude_qualquer_lon_aproximadamente_111_km(self) -> None:
        distancia = distancia_haversine_km(45.0, -30.0, 46.0, -30.0)
        assert 111.0 < distancia < 111.5


class TestBBoxAproximadoPorRaio:
    """Validações do BBOX aproximado em graus."""

    def test_delta_lat_proporcional_a_raio(self) -> None:
        lat_min, lat_max, _lon_min, _lon_max = bbox_aproximado_por_raio(0.0, 0.0, 100.0)
        delta = lat_max - lat_min
        # 100 km ≈ 100 / 111 = 0.9009 graus (em cada lado → largura 1.8019).
        assert delta == pytest.approx(200.0 / 111.0, rel=1e-6)

    def test_bbox_contem_todos_os_pontos_a_raio_menor(self) -> None:
        centro_lat, centro_lon, raio = -23.55, -46.63, 50.0
        lat_min, lat_max, lon_min, lon_max = bbox_aproximado_por_raio(centro_lat, centro_lon, raio)
        # Sortear alguns pontos dentro do círculo exato e checar se caem no BBOX.
        for dlat_fr, dlon_fr in [
            (0.0, 0.0),
            (0.3, 0.0),
            (0.0, 0.3),
            (-0.3, 0.2),
            (0.2, -0.3),
        ]:
            lat = centro_lat + dlat_fr * (raio / 111.0)
            lon = centro_lon + dlon_fr * (raio / (111.0 * math.cos(math.radians(centro_lat))))
            dist = distancia_haversine_km(centro_lat, centro_lon, lat, lon)
            if dist <= raio:
                assert lat_min <= lat <= lat_max
                assert lon_min <= lon <= lon_max

    def test_longitude_mais_larga_em_latitudes_altas(self) -> None:
        _, _, lon_min_eq, lon_max_eq = bbox_aproximado_por_raio(0.0, 0.0, 100.0)
        _, _, lon_min_alta, lon_max_alta = bbox_aproximado_por_raio(60.0, 0.0, 100.0)
        largura_eq = lon_max_eq - lon_min_eq
        largura_alta = lon_max_alta - lon_min_alta
        # cos(60°) = 0.5 → largura em alta latitude ~2x a do equador.
        assert largura_alta > largura_eq * 1.8

    def test_saturacao_proximo_aos_polos(self) -> None:
        # Em lat ~90° cos → ~0; saturamos em 0.01 → delta_lon muito largo.
        _, _, lon_min, lon_max = bbox_aproximado_por_raio(89.9, 0.0, 1.0)
        # delta_lon = 1 / (111 * 0.01) ≈ 0.9 graus → BBox finito, não infinito.
        assert lon_max - lon_min == pytest.approx(2.0 / (111.0 * 0.01), rel=1e-6)

    def test_raio_zero_produz_bbox_degenerado(self) -> None:
        lat_min, lat_max, lon_min, lon_max = bbox_aproximado_por_raio(10.0, 20.0, 0.0)
        assert lat_min == lat_max == 10.0
        assert lon_min == lon_max == 20.0
