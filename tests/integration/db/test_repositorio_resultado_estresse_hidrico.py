"""Testes de :class:`SQLAlchemyRepositorioResultadoEstresseHidrico` (Slice 15)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.core.ids import gerar_id
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.municipio import Municipio
from climate_risk.domain.entidades.resultado_estresse_hidrico import (
    ResultadoEstresseHidrico,
)
from climate_risk.infrastructure.db.repositorios import (
    SQLAlchemyRepositorioExecucoes,
    SQLAlchemyRepositorioMunicipios,
    SQLAlchemyRepositorioResultadoEstresseHidrico,
)


async def _criar_execucao(sessao: AsyncSession, *, cenario: str = "rcp45") -> str:
    repo = SQLAlchemyRepositorioExecucoes(sessao)
    execucao = Execucao(
        id=gerar_id("exec"),
        cenario=cenario,
        variavel="pr+tas+evap",
        arquivo_origem="/dados/pr.nc",
        tipo="estresse_hidrico",
        parametros={},
        status=StatusExecucao.COMPLETED,
        criado_em=datetime(2026, 4, 16, tzinfo=UTC),
        concluido_em=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        job_id=None,
    )
    await repo.salvar(execucao)
    return execucao.id


async def _criar_municipio(sessao: AsyncSession, *, id_: int, nome: str, uf: str) -> None:
    repo = SQLAlchemyRepositorioMunicipios(sessao)
    await repo.salvar(
        Municipio(
            id=id_,
            nome=nome,
            nome_normalizado=nome.lower(),
            uf=uf,
            lat_centroide=None,
            lon_centroide=None,
            atualizado_em=datetime(2026, 4, 1, tzinfo=UTC),
        )
    )


def _mk(
    *,
    execucao_id: str,
    municipio_id: int,
    ano: int,
    cenario: str = "rcp45",
    freq: int = 10,
    intens: float = 12.5,
) -> ResultadoEstresseHidrico:
    return ResultadoEstresseHidrico(
        id=gerar_id("reh"),
        execucao_id=execucao_id,
        municipio_id=municipio_id,
        ano=ano,
        cenario=cenario,
        frequencia_dias_secos_quentes=freq,
        intensidade_mm_dia=intens,
        criado_em=datetime(2026, 4, 24, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_salvar_lote_e_listar(async_session: AsyncSession) -> None:
    execucao_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)

    resultados = [
        _mk(execucao_id=execucao_id, municipio_id=3550308, ano=ano) for ano in (2026, 2027, 2028)
    ]
    await repo.salvar_lote(resultados)

    lidos = await repo.listar(execucao_id=execucao_id)
    assert len(lidos) == 3
    assert {r.ano for r in lidos} == {2026, 2027, 2028}

    total = await repo.contar(execucao_id=execucao_id)
    assert total == 3


@pytest.mark.asyncio
async def test_salvar_lote_vazio_noop(async_session: AsyncSession) -> None:
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)
    await repo.salvar_lote([])
    total = await repo.contar()
    assert total == 0


@pytest.mark.asyncio
async def test_filtros_por_cenario_ano_range_e_municipio(
    async_session: AsyncSession,
) -> None:
    exec_a = await _criar_execucao(async_session, cenario="rcp45")
    exec_b = await _criar_execucao(async_session, cenario="rcp85")
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)

    await repo.salvar_lote(
        [
            _mk(execucao_id=exec_a, municipio_id=3550308, ano=2026, cenario="rcp45"),
            _mk(execucao_id=exec_a, municipio_id=3304557, ano=2026, cenario="rcp45"),
            _mk(execucao_id=exec_a, municipio_id=3550308, ano=2030, cenario="rcp45"),
            _mk(execucao_id=exec_b, municipio_id=3550308, ano=2026, cenario="rcp85"),
        ]
    )

    assert await repo.contar(cenario="rcp45") == 3
    assert await repo.contar(ano=2026) == 3
    assert await repo.contar(ano_min=2027) == 1
    assert await repo.contar(ano_min=2026, ano_max=2029) == 3
    assert await repo.contar(municipio_id=3550308) == 3


@pytest.mark.asyncio
async def test_filtro_por_uf_faz_join_com_municipio(
    async_session: AsyncSession,
) -> None:
    await _criar_municipio(async_session, id_=3550308, nome="São Paulo", uf="SP")
    await _criar_municipio(async_session, id_=3304557, nome="Rio de Janeiro", uf="RJ")
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)

    await repo.salvar_lote(
        [
            _mk(execucao_id=exec_id, municipio_id=3550308, ano=2026),
            _mk(execucao_id=exec_id, municipio_id=3304557, ano=2026),
            _mk(execucao_id=exec_id, municipio_id=3550308, ano=2027),
        ]
    )

    assert await repo.contar(uf="SP") == 2
    assert await repo.contar(uf="RJ") == 1


@pytest.mark.asyncio
async def test_listar_com_municipio_enriquece_com_nome_uf(
    async_session: AsyncSession,
) -> None:
    await _criar_municipio(async_session, id_=3550308, nome="São Paulo", uf="SP")
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)
    await repo.salvar_lote(
        [
            _mk(execucao_id=exec_id, municipio_id=3550308, ano=2026),
            _mk(execucao_id=exec_id, municipio_id=9999999, ano=2026),
        ]
    )
    linhas = await repo.listar_com_municipio()
    nomes = {r.municipio_id: (nome, uf) for r, nome, uf in linhas}
    assert nomes[3550308] == ("São Paulo", "SP")
    # Município sem cadastro: LEFT JOIN devolve None.
    assert nomes[9999999] == (None, None)


@pytest.mark.asyncio
async def test_unique_constraint_impede_duplicatas(async_session: AsyncSession) -> None:
    exec_id = await _criar_execucao(async_session)
    repo = SQLAlchemyRepositorioResultadoEstresseHidrico(async_session)

    await repo.salvar_lote([_mk(execucao_id=exec_id, municipio_id=3550308, ano=2026)])
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        await repo.salvar_lote([_mk(execucao_id=exec_id, municipio_id=3550308, ano=2026, freq=99)])
