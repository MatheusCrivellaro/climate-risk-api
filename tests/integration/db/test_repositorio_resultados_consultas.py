"""Testes de integração das novas consultas de ``SQLAlchemyRepositorioResultados`` (Slice 11)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
)
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioMunicipios,
    SQLAlchemyRepositorioResultados,
)


async def _criar_execucao(
    sessao: AsyncSession, cenario: str = "rcp45", variavel: str = "pr"
) -> str:
    repo = SQLAlchemyRepositorioExecucoes(sessao)
    execucao = Execucao(
        id=gerar_id("exec"),
        cenario=cenario,
        variavel=variavel,
        arquivo_origem="/dados/pr.nc",
        tipo="grade_bbox",
        parametros={},
        status=StatusExecucao.COMPLETED,
        criado_em=datetime(2026, 4, 16, tzinfo=UTC),
        concluido_em=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        job_id=None,
    )
    await repo.salvar(execucao)
    return execucao.id


async def _criar_municipio(sessao: AsyncSession, id_mun: int = 3550308, uf: str = "SP") -> int:
    repo = SQLAlchemyRepositorioMunicipios(sessao)
    mun = Municipio(
        id=id_mun,
        nome=f"Municipio {id_mun}",
        nome_normalizado=f"municipio {id_mun}",
        uf=uf,
        lat_centroide=-23.55,
        lon_centroide=-46.63,
        atualizado_em=datetime(2026, 4, 16, tzinfo=UTC),
    )
    await repo.salvar(mun)
    return mun.id


def _res(
    execucao_id: str,
    *,
    lat: float = -23.5,
    lon: float = -46.6,
    ano: int = 2026,
    nome: str = "PRCPTOT",
    valor: float | None = 10.0,
    municipio_id: int | None = None,
) -> ResultadoIndice:
    return ResultadoIndice(
        id=gerar_id("res"),
        execucao_id=execucao_id,
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


@pytest.mark.asyncio
async def test_consultar_com_filtros_diversos(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session, cenario="rcp45", variavel="pr")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_id, ano=2026, nome="PRCPTOT", valor=100.0),
            _res(exec_id, ano=2026, nome="CDD", valor=15.0),
            _res(exec_id, ano=2027, nome="PRCPTOT", valor=110.0),
            _res(exec_id, ano=2027, nome="CDD", valor=14.0),
            _res(exec_id, ano=2028, nome="PRCPTOT", valor=105.0),
        ]
    )

    # ano exato + IN(...).
    itens = await repo.consultar(
        FiltrosConsultaResultados(execucao_id=exec_id, ano=2026, nomes_indices=("PRCPTOT",))
    )
    assert len(itens) == 1
    assert itens[0].ano == 2026 and itens[0].nome_indice == "PRCPTOT"

    # intervalo de anos + IN multiplo.
    itens = await repo.consultar(
        FiltrosConsultaResultados(
            execucao_id=exec_id,
            ano_min=2026,
            ano_max=2027,
            nomes_indices=("PRCPTOT", "CDD"),
        )
    )
    assert len(itens) == 4

    total = await repo.contar_por_filtros(FiltrosConsultaResultados(execucao_id=exec_id))
    assert total == 5


@pytest.mark.asyncio
async def test_consultar_com_cenario_variavel_via_join(
    async_session: AsyncSession,
) -> None:
    exec_45 = await _criar_execucao(async_session, cenario="rcp45", variavel="pr")
    exec_85 = await _criar_execucao(async_session, cenario="rcp85", variavel="pr")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_45, nome="PRCPTOT"),
            _res(exec_45, nome="CDD"),
            _res(exec_85, nome="PRCPTOT"),
        ]
    )

    itens = await repo.consultar(FiltrosConsultaResultados(cenario="rcp85", variavel="pr"))
    assert len(itens) == 1
    assert itens[0].execucao_id == exec_85


@pytest.mark.asyncio
async def test_consultar_com_uf_via_join_municipio(
    async_session: AsyncSession,
) -> None:
    exec_id = await _criar_execucao(async_session)
    mun_sp = await _criar_municipio(async_session, id_mun=3550308, uf="SP")
    mun_rj = await _criar_municipio(async_session, id_mun=3304557, uf="RJ")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_id, municipio_id=mun_sp),
            _res(exec_id, municipio_id=mun_rj),
            _res(exec_id, municipio_id=None),
        ]
    )

    itens = await repo.consultar(FiltrosConsultaResultados(uf="SP"))
    assert len(itens) == 1
    assert itens[0].municipio_id == mun_sp


@pytest.mark.asyncio
async def test_consultar_bbox_simples_e_antimeridiano(
    async_session: AsyncSession,
) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_id, lat=0.0, lon=170.0),  # fiji-ish
            _res(exec_id, lat=0.0, lon=-175.0),  # cruzou antimeridiano
            _res(exec_id, lat=0.0, lon=0.0),  # greenwich
        ]
    )

    # BBox normal.
    itens = await repo.consultar(
        FiltrosConsultaResultados(lat_min=-1.0, lat_max=1.0, lon_min=-5.0, lon_max=5.0)
    )
    assert len(itens) == 1

    # BBox cruzando antimeridiano (lon_min > lon_max).
    itens = await repo.consultar(
        FiltrosConsultaResultados(lat_min=-1.0, lat_max=1.0, lon_min=165.0, lon_max=-170.0)
    )
    assert len(itens) == 2
    assert {round(r.lon) for r in itens} == {170, -175}


@pytest.mark.asyncio
async def test_paginacao(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote([_res(exec_id, ano=2020 + i) for i in range(5)])

    pag1 = await repo.consultar(FiltrosConsultaResultados(execucao_id=exec_id), limit=2, offset=0)
    pag2 = await repo.consultar(FiltrosConsultaResultados(execucao_id=exec_id), limit=2, offset=2)
    assert len(pag1) == 2
    assert len(pag2) == 2
    ids = {r.id for r in pag1} | {r.id for r in pag2}
    assert len(ids) == 4


@pytest.mark.asyncio
async def test_agregar_media_por_ano(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_id, ano=2026, valor=10.0),
            _res(exec_id, ano=2026, valor=20.0),
            _res(exec_id, ano=2027, valor=30.0),
            _res(exec_id, ano=2027, valor=40.0),
        ]
    )

    grupos = await repo.agregar(
        FiltrosAgregacaoResultados(
            filtros=FiltrosConsultaResultados(execucao_id=exec_id),
            agregacao="media",
            agrupar_por=("ano",),
        )
    )
    mapa = {g.grupo["ano"]: g for g in grupos}
    assert mapa[2026].valor == pytest.approx(15.0)
    assert mapa[2027].valor == pytest.approx(35.0)
    assert mapa[2026].n_amostras == 2


@pytest.mark.asyncio
async def test_agregar_count_inclui_nulos(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_id, valor=10.0),
            _res(exec_id, valor=None),
            _res(exec_id, valor=30.0),
        ]
    )
    grupos = await repo.agregar(
        FiltrosAgregacaoResultados(
            filtros=FiltrosConsultaResultados(execucao_id=exec_id),
            agregacao="count",
            agrupar_por=(),
        )
    )
    assert len(grupos) == 1
    assert grupos[0].valor == 3.0
    assert grupos[0].n_amostras == 3


@pytest.mark.asyncio
async def test_agregar_percentil_em_python(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote([_res(exec_id, valor=float(v)) for v in range(1, 101)])

    grupos = await repo.agregar(
        FiltrosAgregacaoResultados(
            filtros=FiltrosConsultaResultados(execucao_id=exec_id),
            agregacao="p50",
            agrupar_por=(),
        )
    )
    assert len(grupos) == 1
    assert 45.0 <= (grupos[0].valor or 0) <= 55.0

    grupos = await repo.agregar(
        FiltrosAgregacaoResultados(
            filtros=FiltrosConsultaResultados(execucao_id=exec_id),
            agregacao="p95",
            agrupar_por=(),
        )
    )
    assert 90.0 <= (grupos[0].valor or 0) <= 99.0


@pytest.mark.asyncio
async def test_agregar_por_cenario_usa_join(async_session: AsyncSession) -> None:
    exec_45 = await _criar_execucao(async_session, cenario="rcp45", variavel="pr")
    exec_85 = await _criar_execucao(async_session, cenario="rcp85", variavel="pr")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_45, valor=10.0),
            _res(exec_45, valor=20.0),
            _res(exec_85, valor=30.0),
        ]
    )

    grupos = await repo.agregar(
        FiltrosAgregacaoResultados(
            filtros=FiltrosConsultaResultados(),
            agregacao="media",
            agrupar_por=("cenario",),
        )
    )
    mapa = {g.grupo["cenario"]: g for g in grupos}
    assert mapa["rcp45"].valor == pytest.approx(15.0)
    assert mapa["rcp85"].valor == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_distinct_e_counters(async_session: AsyncSession) -> None:
    exec_45 = await _criar_execucao(async_session, cenario="rcp45", variavel="pr")
    exec_85 = await _criar_execucao(async_session, cenario="rcp85", variavel="tas")
    repo = SQLAlchemyRepositorioResultados(async_session)

    await repo.salvar_lote(
        [
            _res(exec_45, ano=2026, nome="PRCPTOT"),
            _res(exec_45, ano=2027, nome="CDD"),
            _res(exec_85, ano=2026, nome="TNx"),
        ]
    )

    assert sorted(await repo.distinct_cenarios()) == ["rcp45", "rcp85"]
    assert await repo.distinct_anos() == [2026, 2027]
    assert sorted(await repo.distinct_variaveis()) == ["pr", "tas"]
    assert sorted(await repo.distinct_nomes_indices()) == ["CDD", "PRCPTOT", "TNx"]
    assert await repo.contar_execucoes_com_resultados() == 2
    assert await repo.contar_resultados() == 3


@pytest.mark.asyncio
async def test_agregacao_invalida_levanta(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote([_res(exec_id, valor=10.0)])

    with pytest.raises(ValueError, match="Agregação inválida"):
        await repo.agregar(
            FiltrosAgregacaoResultados(
                filtros=FiltrosConsultaResultados(execucao_id=exec_id),
                agregacao="soma",
            )
        )


@pytest.mark.asyncio
async def test_dimensao_agrupar_por_invalida_levanta(
    async_session: AsyncSession,
) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultados(async_session)
    await repo.salvar_lote([_res(exec_id, valor=10.0)])

    with pytest.raises(ValueError, match="Dimensão"):
        await repo.agregar(
            FiltrosAgregacaoResultados(
                filtros=FiltrosConsultaResultados(execucao_id=exec_id),
                agregacao="media",
                agrupar_por=("foobar",),
            )
        )
