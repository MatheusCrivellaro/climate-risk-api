"""Caso de uso :class:`AnalisarCoberturaFornecedores` (UC-04 — Slice 9).

Para cada fornecedor (``identificador`` + ``cidade/uf``), responde:

1. O município foi geocodificado (via :class:`GeocodificarLocalizacoes`)?
2. Se sim, temos ``ResultadoIndice`` persistido para esse município?

Combinação das duas perguntas dá a *cobertura*:

- ``municipio_nao_geocodificado`` — falhou em ``cache → fuzzy → IBGE``.
- ``sem_dados_climaticos`` — existe no IBGE, mas nunca foi processado.
- ``tem_cobertura=True`` — existe e há dados climáticos associados.

Reaproveita :class:`GeocodificarLocalizacoes` do Slice 8 sem duplicar
lógica — o batching por UF, cache fuzzy e degradação de IBGE vêm de graça.
"""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.application.geocodificacao import (
    EntradaLocalizacao,
    GeocodificarLocalizacoes,
)
from climate_risk.domain.portas.repositorios import RepositorioResultados


@dataclass(frozen=True)
class FornecedorEntrada:
    """Entrada estruturada — equivalente a uma linha do CSV legado."""

    identificador: str
    cidade: str
    uf: str


@dataclass(frozen=True)
class FornecedorCobertura:
    """Resultado unitário."""

    identificador: str
    cidade_entrada: str
    uf_entrada: str
    tem_cobertura: bool
    municipio_id: int | None
    nome_canonico: str | None
    motivo_nao_encontrado: str | None


@dataclass(frozen=True)
class ResultadoCobertura:
    """Sumário do lote."""

    total: int
    com_cobertura: int
    sem_cobertura: int
    itens: list[FornecedorCobertura]


MOTIVO_NAO_GEOCODIFICADO = "municipio_nao_geocodificado"
MOTIVO_SEM_DADOS = "sem_dados_climaticos"


class AnalisarCoberturaFornecedores:
    """Combina geocodificação (Slice 8) com consulta em resultados."""

    def __init__(
        self,
        geocodificar: GeocodificarLocalizacoes,
        repositorio_resultados: RepositorioResultados,
    ) -> None:
        self._geo = geocodificar
        self._repo = repositorio_resultados

    async def executar(self, fornecedores: list[FornecedorEntrada]) -> ResultadoCobertura:
        if not fornecedores:
            return ResultadoCobertura(total=0, com_cobertura=0, sem_cobertura=0, itens=[])

        entradas_geo = [EntradaLocalizacao(cidade=f.cidade, uf=f.uf) for f in fornecedores]
        resultado_geo = await self._geo.executar(entradas_geo)

        municipios_ids = {
            item.municipio_id for item in resultado_geo.itens if item.municipio_id is not None
        }
        com_dados = (
            await self._repo.municipios_com_resultados(municipios_ids) if municipios_ids else set()
        )

        itens: list[FornecedorCobertura] = []
        com_cobertura = 0
        for fornecedor, geo in zip(fornecedores, resultado_geo.itens, strict=True):
            if geo.municipio_id is None:
                itens.append(
                    FornecedorCobertura(
                        identificador=fornecedor.identificador,
                        cidade_entrada=fornecedor.cidade,
                        uf_entrada=fornecedor.uf,
                        tem_cobertura=False,
                        municipio_id=None,
                        nome_canonico=None,
                        motivo_nao_encontrado=MOTIVO_NAO_GEOCODIFICADO,
                    )
                )
                continue
            if geo.municipio_id not in com_dados:
                itens.append(
                    FornecedorCobertura(
                        identificador=fornecedor.identificador,
                        cidade_entrada=fornecedor.cidade,
                        uf_entrada=fornecedor.uf,
                        tem_cobertura=False,
                        municipio_id=geo.municipio_id,
                        nome_canonico=geo.nome_canonico,
                        motivo_nao_encontrado=MOTIVO_SEM_DADOS,
                    )
                )
                continue
            com_cobertura += 1
            itens.append(
                FornecedorCobertura(
                    identificador=fornecedor.identificador,
                    cidade_entrada=fornecedor.cidade,
                    uf_entrada=fornecedor.uf,
                    tem_cobertura=True,
                    municipio_id=geo.municipio_id,
                    nome_canonico=geo.nome_canonico,
                    motivo_nao_encontrado=None,
                )
            )

        return ResultadoCobertura(
            total=len(itens),
            com_cobertura=com_cobertura,
            sem_cobertura=len(itens) - com_cobertura,
            itens=itens,
        )
