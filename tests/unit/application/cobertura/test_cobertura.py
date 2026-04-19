"""Testes de :class:`AnalisarCoberturaFornecedores`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from climate_risk.application.cobertura import (
    AnalisarCoberturaFornecedores,
    FornecedorEntrada,
)
from climate_risk.application.cobertura.cobertura_fornecedores import (
    MOTIVO_NAO_GEOCODIFICADO,
    MOTIVO_SEM_DADOS,
)
from climate_risk.application.geocodificacao import (
    EntradaLocalizacao,
    LocalizacaoGeocodificada,
    ResultadoGeocodificacao,
)


@dataclass
class _GeoFake:
    mapa_por_entrada: dict[tuple[str, str], LocalizacaoGeocodificada] = field(default_factory=dict)
    entradas_vistas: list[EntradaLocalizacao] = field(default_factory=list)

    async def executar(self, entradas: list[EntradaLocalizacao]) -> ResultadoGeocodificacao:
        self.entradas_vistas = list(entradas)
        itens = []
        for e in entradas:
            chave = (e.cidade, e.uf)
            if chave in self.mapa_por_entrada:
                itens.append(self.mapa_por_entrada[chave])
            else:
                itens.append(
                    LocalizacaoGeocodificada(
                        cidade_entrada=e.cidade,
                        uf=e.uf,
                        municipio_id=None,
                        nome_canonico=None,
                        lat=None,
                        lon=None,
                        metodo="nao_encontrado",
                    )
                )
        encontrados = sum(1 for i in itens if i.municipio_id is not None)
        return ResultadoGeocodificacao(
            total=len(itens),
            encontrados=encontrados,
            nao_encontrados=len(itens) - encontrados,
            itens=itens,
        )


@dataclass
class _RepoFake:
    municipios_com_dados: set[int] = field(default_factory=set)
    ids_consultados: set[int] | None = None

    async def salvar_lote(self, _: Sequence[object]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def listar(self, **_: object) -> list[object]:  # pragma: no cover
        return []

    async def contar(self, **_: object) -> int:  # pragma: no cover
        return 0

    async def municipios_com_resultados(self, municipios_ids: set[int]) -> set[int]:
        self.ids_consultados = municipios_ids
        return municipios_ids & self.municipios_com_dados


def _geocodificado(cidade: str, uf: str, municipio_id: int, nome: str) -> LocalizacaoGeocodificada:
    return LocalizacaoGeocodificada(
        cidade_entrada=cidade,
        uf=uf,
        municipio_id=municipio_id,
        nome_canonico=nome,
        lat=-23.0,
        lon=-46.0,
        metodo="cache_exato",
    )


@pytest.mark.asyncio
async def test_tres_cenarios_motivos_diferentes() -> None:
    geo = _GeoFake(
        mapa_por_entrada={
            ("São Paulo", "SP"): _geocodificado("São Paulo", "SP", 3550308, "São Paulo"),
            ("Rio de Janeiro", "RJ"): _geocodificado("Rio de Janeiro", "RJ", 3304557, "Rio"),
            # "XYZ/ZZ" fica sem geocodificação.
        }
    )
    repo = _RepoFake(municipios_com_dados={3550308})
    caso = AnalisarCoberturaFornecedores(geocodificar=geo, repositorio_resultados=repo)

    fornecedores = [
        FornecedorEntrada(identificador="f1", cidade="São Paulo", uf="SP"),
        FornecedorEntrada(identificador="f2", cidade="Rio de Janeiro", uf="RJ"),
        FornecedorEntrada(identificador="f3", cidade="XYZ", uf="ZZ"),
    ]
    resultado = await caso.executar(fornecedores)

    assert resultado.total == 3
    assert resultado.com_cobertura == 1
    assert resultado.sem_cobertura == 2

    por_id = {i.identificador: i for i in resultado.itens}
    assert por_id["f1"].tem_cobertura is True
    assert por_id["f1"].municipio_id == 3550308
    assert por_id["f1"].motivo_nao_encontrado is None

    assert por_id["f2"].tem_cobertura is False
    assert por_id["f2"].motivo_nao_encontrado == MOTIVO_SEM_DADOS
    assert por_id["f2"].municipio_id == 3304557

    assert por_id["f3"].tem_cobertura is False
    assert por_id["f3"].motivo_nao_encontrado == MOTIVO_NAO_GEOCODIFICADO
    assert por_id["f3"].municipio_id is None


@pytest.mark.asyncio
async def test_lote_vazio_nao_invoca_dependencias() -> None:
    geo = _GeoFake()
    repo = _RepoFake()
    caso = AnalisarCoberturaFornecedores(geocodificar=geo, repositorio_resultados=repo)
    resultado = await caso.executar([])
    assert resultado.total == 0
    assert resultado.com_cobertura == 0
    assert resultado.sem_cobertura == 0
    assert repo.ids_consultados is None
    assert geo.entradas_vistas == []


@pytest.mark.asyncio
async def test_municipios_duplicados_consultam_unicos() -> None:
    """Se dois fornecedores geocodificam para o mesmo município, consulta só um ID."""
    geo = _GeoFake(
        mapa_por_entrada={
            ("São Paulo", "SP"): _geocodificado("São Paulo", "SP", 3550308, "São Paulo"),
            ("SP", "SP"): _geocodificado("SP", "SP", 3550308, "São Paulo"),
        }
    )
    repo = _RepoFake(municipios_com_dados={3550308})
    caso = AnalisarCoberturaFornecedores(geocodificar=geo, repositorio_resultados=repo)

    resultado = await caso.executar(
        [
            FornecedorEntrada(identificador="a", cidade="São Paulo", uf="SP"),
            FornecedorEntrada(identificador="b", cidade="SP", uf="SP"),
        ]
    )
    assert repo.ids_consultados == {3550308}
    assert all(i.tem_cobertura for i in resultado.itens)


@pytest.mark.asyncio
async def test_nao_geocodificado_nao_entra_na_consulta_sql() -> None:
    geo = _GeoFake()  # nada geocodifica
    repo = _RepoFake()
    caso = AnalisarCoberturaFornecedores(geocodificar=geo, repositorio_resultados=repo)
    resultado = await caso.executar([FornecedorEntrada(identificador="a", cidade="X", uf="YY")])
    assert repo.ids_consultados is None
    assert resultado.itens[0].motivo_nao_encontrado == MOTIVO_NAO_GEOCODIFICADO
