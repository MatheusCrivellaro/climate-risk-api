"""Testes unitários de :class:`ConsultarResultados`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.resultados import ConsultarResultados, FiltrosResultados
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.excecoes import ErroValidacao
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
    GrupoAgregadoRaw,
)


def _r(
    rid: str,
    lat: float,
    lon: float,
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
    consultas: list[tuple[FiltrosConsultaResultados, int, int]] = field(default_factory=list)
    agregacoes: list[FiltrosAgregacaoResultados] = field(default_factory=list)

    async def salvar_lote(self, resultados: Sequence[ResultadoIndice]) -> None:
        self.items.extend(resultados)

    async def listar(self, **_: object) -> list[ResultadoIndice]:
        return list(self.items)

    async def contar(self, **_: object) -> int:
        return len(self.items)

    async def municipios_com_resultados(self, municipios_ids: set[int]) -> set[int]:
        return {i.municipio_id for i in self.items if i.municipio_id in municipios_ids}

    async def consultar(
        self,
        filtros: FiltrosConsultaResultados,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        self.consultas.append((filtros, limit, offset))
        itens = self._aplicar(filtros)
        return itens[offset : offset + limit]

    async def contar_por_filtros(self, filtros: FiltrosConsultaResultados) -> int:
        return len(self._aplicar(filtros))

    async def agregar(
        self, filtros_agregacao: FiltrosAgregacaoResultados
    ) -> list[GrupoAgregadoRaw]:
        self.agregacoes.append(filtros_agregacao)
        return []

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
        return len(self.items)

    def _aplicar(self, filtros: FiltrosConsultaResultados) -> list[ResultadoIndice]:
        itens = list(self.items)
        if filtros.execucao_id is not None:
            itens = [i for i in itens if i.execucao_id == filtros.execucao_id]
        if filtros.ano is not None:
            itens = [i for i in itens if i.ano == filtros.ano]
        if filtros.ano_min is not None:
            itens = [i for i in itens if i.ano >= filtros.ano_min]
        if filtros.ano_max is not None:
            itens = [i for i in itens if i.ano <= filtros.ano_max]
        if filtros.nomes_indices:
            itens = [i for i in itens if i.nome_indice in filtros.nomes_indices]
        if filtros.municipio_id is not None:
            itens = [i for i in itens if i.municipio_id == filtros.municipio_id]
        if filtros.lat_min is not None and filtros.lat_max is not None:
            itens = [i for i in itens if filtros.lat_min <= i.lat <= filtros.lat_max]
        if filtros.lon_min is not None and filtros.lon_max is not None:
            if filtros.lon_min <= filtros.lon_max:
                itens = [i for i in itens if filtros.lon_min <= i.lon <= filtros.lon_max]
            else:
                itens = [
                    i for i in itens if i.lon >= filtros.lon_min or i.lon <= filtros.lon_max
                ]
        return itens


@pytest.mark.asyncio
async def test_consulta_simples_delega_ao_repo() -> None:
    repo = _RepoFake(items=[_r("r1", -23.5, -46.6), _r("r2", -22.9, -43.2)])
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.executar(FiltrosResultados(limit=10, offset=0))

    assert pagina.total == 2
    assert [r.id for r in pagina.items] == ["r1", "r2"]


@pytest.mark.asyncio
async def test_filtro_por_ano_nomes_indices() -> None:
    repo = _RepoFake(
        items=[
            _r("a", 0.0, 0.0, ano=2026, nome="PRCPTOT"),
            _r("b", 0.0, 0.0, ano=2027, nome="CDD"),
            _r("c", 0.0, 0.0, ano=2026, nome="CDD"),
        ]
    )
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.executar(
        FiltrosResultados(ano=2026, nomes_indices=("CDD",), limit=10)
    )

    assert [r.id for r in pagina.items] == ["c"]


@pytest.mark.asyncio
async def test_raio_aplica_haversine_apos_bbox() -> None:
    # Centro: SP (-23.55, -46.63). Rio fica a ~358 km.
    repo = _RepoFake(
        items=[
            _r("sp", -23.55, -46.63),  # 0 km (in)
            _r("campinas", -22.91, -47.06),  # ~80 km (in)
            _r("rio", -22.91, -43.21),  # ~358 km (out p/ raio 200)
            _r("belem", -1.46, -48.49),  # longe (out)
        ]
    )
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.executar(
        FiltrosResultados(
            raio_km=200.0,
            centro_lat=-23.55,
            centro_lon=-46.63,
            limit=100,
        )
    )

    ids = {r.id for r in pagina.items}
    assert ids == {"sp", "campinas"}
    assert pagina.total == 2


@pytest.mark.asyncio
async def test_raio_exige_centro_latlon() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="raio_km"):
        await caso.executar(FiltrosResultados(raio_km=10.0))


@pytest.mark.asyncio
async def test_raio_negativo_rejeitado() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="raio_km"):
        await caso.executar(
            FiltrosResultados(raio_km=-1.0, centro_lat=0.0, centro_lon=0.0)
        )


@pytest.mark.asyncio
async def test_ano_exclusivo_com_anomin_anomax() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="ano"):
        await caso.executar(FiltrosResultados(ano=2026, ano_min=2025))


@pytest.mark.asyncio
async def test_ano_min_maior_que_ano_max_rejeitado() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="ano_min"):
        await caso.executar(FiltrosResultados(ano_min=2030, ano_max=2020))


@pytest.mark.asyncio
async def test_limit_acima_do_maximo_rejeitado() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="limit"):
        await caso.executar(FiltrosResultados(limit=1001))


@pytest.mark.asyncio
async def test_offset_negativo_rejeitado() -> None:
    repo = _RepoFake()
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    with pytest.raises(ErroValidacao, match="offset"):
        await caso.executar(FiltrosResultados(offset=-1))


@pytest.mark.asyncio
async def test_paginacao_no_caminho_raio() -> None:
    repo = _RepoFake(
        items=[
            _r("a", -23.55, -46.63),
            _r("b", -23.56, -46.64),
            _r("c", -23.57, -46.65),
        ]
    )
    caso = ConsultarResultados(repositorio=repo)  # type: ignore[arg-type]

    pagina = await caso.executar(
        FiltrosResultados(
            raio_km=50.0,
            centro_lat=-23.55,
            centro_lon=-46.63,
            limit=2,
            offset=1,
        )
    )
    assert pagina.total == 3
    assert len(pagina.items) == 2
    assert [r.id for r in pagina.items] == ["b", "c"]
