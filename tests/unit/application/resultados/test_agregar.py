"""Testes unitários de :class:`AgregarResultados`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.resultados import AgregarResultados, FiltrosAgregacao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.excecoes import ErroValidacao
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
    GrupoAgregadoRaw,
)


def _r(
    rid: str,
    lat: float = 0.0,
    lon: float = 0.0,
    ano: int = 2026,
    nome: str = "PRCPTOT",
    valor: float | None = 10.0,
    municipio_id: int | None = None,
) -> ResultadoIndice:
    return ResultadoIndice(
        id=rid,
        execucao_id="exec_1",
        lat=lat,
        lon=lon,
        lat_input=lat,
        lon_input=lon,
        ano=ano,
        nome_indice=nome,
        valor=valor,
        unidade="mm",
        municipio_id=municipio_id,
    )


@dataclass
class _RepoFake:
    items: list[ResultadoIndice] = field(default_factory=list)
    agregacoes_recebidas: list[FiltrosAgregacaoResultados] = field(default_factory=list)
    resposta_agregacao: list[GrupoAgregadoRaw] = field(default_factory=list)

    async def salvar_lote(self, _: Sequence[ResultadoIndice]) -> None:
        return None

    async def listar(self, **_: object) -> list[ResultadoIndice]:
        return []

    async def contar(self, **_: object) -> int:
        return 0

    async def municipios_com_resultados(self, _: set[int]) -> set[int]:
        return set()

    async def consultar(
        self,
        filtros: FiltrosConsultaResultados,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        itens = list(self.items)
        if filtros.lat_min is not None and filtros.lat_max is not None:
            itens = [i for i in itens if filtros.lat_min <= i.lat <= filtros.lat_max]
        if filtros.lon_min is not None and filtros.lon_max is not None:
            itens = [i for i in itens if filtros.lon_min <= i.lon <= filtros.lon_max]
        return itens[offset : offset + limit]

    async def contar_por_filtros(self, _: FiltrosConsultaResultados) -> int:
        return 0

    async def agregar(
        self, filtros_agregacao: FiltrosAgregacaoResultados
    ) -> list[GrupoAgregadoRaw]:
        self.agregacoes_recebidas.append(filtros_agregacao)
        return list(self.resposta_agregacao)

    async def distinct_cenarios(self) -> list[str]:
        return []

    async def distinct_anos(self) -> list[int]:
        return []

    async def distinct_variaveis(self) -> list[str]:
        return []

    async def distinct_nomes_indices(self) -> list[str]:
        return []

    async def contar_execucoes_com_resultados(self) -> int:
        return 0

    async def contar_resultados(self) -> int:
        return 0


@pytest.mark.asyncio
async def test_delega_ao_repositorio_e_propaga_grupos() -> None:
    repo = _RepoFake(
        resposta_agregacao=[
            GrupoAgregadoRaw(grupo={"ano": 2026}, valor=12.5, n_amostras=4),
            GrupoAgregadoRaw(grupo={"ano": 2027}, valor=15.0, n_amostras=6),
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(agregacao="media", agrupar_por=("ano",))
    )

    assert resultado.agregacao == "media"
    assert resultado.agrupar_por == ("ano",)
    assert [g.valor for g in resultado.grupos] == [12.5, 15.0]
    assert [g.n_amostras for g in resultado.grupos] == [4, 6]
    assert len(repo.agregacoes_recebidas) == 1


@pytest.mark.asyncio
async def test_agregacao_invalida_rejeitada() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="agregacao"):
        await caso.executar(FiltrosAgregacao(agregacao="inexistente"))


@pytest.mark.asyncio
async def test_dimensao_agrupar_por_invalida_rejeitada() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="agrupar_por"):
        await caso.executar(
            FiltrosAgregacao(agregacao="media", agrupar_por=("foobar",))
        )


@pytest.mark.asyncio
async def test_raio_sem_centro_rejeitado() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="raio_km"):
        await caso.executar(FiltrosAgregacao(raio_km=10.0))


@pytest.mark.asyncio
async def test_caminho_raio_agrega_em_memoria() -> None:
    # Centro: (0, 0). Inclui 4 pontos, 3 dentro do raio de 200 km, 1 fora.
    repo = _RepoFake(
        items=[
            _r("a", lat=0.0, lon=0.0, valor=10.0, nome="PRCPTOT"),
            _r("b", lat=0.1, lon=0.1, valor=20.0, nome="PRCPTOT"),
            _r("c", lat=0.2, lon=0.2, valor=30.0, nome="CDD"),
            _r("d", lat=10.0, lon=10.0, valor=999.0, nome="PRCPTOT"),  # fora
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=200.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="media",
            agrupar_por=("nome_indice",),
        )
    )

    # PRCPTOT: (10 + 20) / 2 = 15. CDD: 30.
    grupos_por_nome = {g.grupo.get("nome_indice"): g for g in resultado.grupos}
    assert grupos_por_nome["PRCPTOT"].valor == pytest.approx(15.0)
    assert grupos_por_nome["PRCPTOT"].n_amostras == 2
    assert grupos_por_nome["CDD"].valor == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_caminho_raio_percentil_p50() -> None:
    repo = _RepoFake(
        items=[
            _r("a", valor=10.0),
            _r("b", valor=20.0),
            _r("c", valor=30.0),
            _r("d", valor=40.0),
            _r("e", valor=50.0),
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="p50",
        )
    )
    # Mediana de [10,20,30,40,50] ≈ 30.
    assert resultado.grupos[0].valor == pytest.approx(30.0, rel=0.05)


@pytest.mark.asyncio
async def test_raio_km_zero_rejeitado() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="raio_km"):
        await caso.executar(
            FiltrosAgregacao(raio_km=0.0, centro_lat=0.0, centro_lon=0.0)
        )


@pytest.mark.asyncio
async def test_ano_exclusivo_com_anomin_anomax() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="ano"):
        await caso.executar(FiltrosAgregacao(ano=2026, ano_max=2027))


@pytest.mark.asyncio
async def test_ano_min_maior_que_ano_max_rejeitado() -> None:
    repo = _RepoFake()
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="ano_min"):
        await caso.executar(FiltrosAgregacao(ano_min=2030, ano_max=2020))


@pytest.mark.asyncio
async def test_caminho_raio_agrega_por_ano() -> None:
    repo = _RepoFake(
        items=[
            _r("a", ano=2026, valor=10.0),
            _r("b", ano=2026, valor=20.0),
            _r("c", ano=2027, valor=30.0),
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="min",
            agrupar_por=("ano",),
        )
    )
    mapa = {g.grupo["ano"]: g for g in resultado.grupos}
    assert mapa[2026].valor == 10.0
    assert mapa[2027].valor == 30.0


@pytest.mark.asyncio
async def test_caminho_raio_max() -> None:
    repo = _RepoFake(items=[_r("a", valor=10.0), _r("b", valor=30.0)])
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="max",
        )
    )
    assert resultado.grupos[0].valor == 30.0


@pytest.mark.asyncio
async def test_caminho_raio_p50_com_um_elemento() -> None:
    repo = _RepoFake(items=[_r("a", valor=42.0)])
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="p50",
        )
    )
    assert resultado.grupos[0].valor == 42.0


@pytest.mark.asyncio
async def test_caminho_raio_agrupar_por_municipio() -> None:
    repo = _RepoFake(
        items=[
            _r("a", valor=10.0, municipio_id=1),
            _r("b", valor=20.0, municipio_id=1),
            _r("c", valor=30.0, municipio_id=2),
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="media",
            agrupar_por=("municipio",),
        )
    )
    mapa = {g.grupo["municipio"]: g for g in resultado.grupos}
    assert mapa[1].valor == pytest.approx(15.0)
    assert mapa[2].valor == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_caminho_raio_agrupar_por_cenario_colapsa_em_bucket_unico() -> None:
    # "cenario" no caminho em memória não é resolvível — cai num único bucket.
    repo = _RepoFake(items=[_r("a", valor=10.0), _r("b", valor=20.0)])
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="media",
            agrupar_por=("cenario",),
        )
    )
    assert len(resultado.grupos) == 1
    # Como a chave é None, nenhuma chave aparece no dict final.
    assert resultado.grupos[0].grupo == {}
    assert resultado.grupos[0].valor == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_caminho_raio_count_inclui_nulos() -> None:
    repo = _RepoFake(
        items=[
            _r("a", valor=10.0),
            _r("b", valor=None),
            _r("c", valor=30.0),
        ]
    )
    caso = AgregarResultados(repositorio=repo)  # type: ignore[arg-type]

    resultado = await caso.executar(
        FiltrosAgregacao(
            raio_km=500.0,
            centro_lat=0.0,
            centro_lon=0.0,
            agregacao="count",
        )
    )
    assert resultado.grupos[0].valor == 3.0
    assert resultado.grupos[0].n_amostras == 3
