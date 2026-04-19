"""Caso de uso :class:`ImportarFornecedores` — import em lote.

Recebe uma lista de :class:`LinhaImportacao` (já parseadas pelo leitor de
CSV/XLSX na camada ``infrastructure``) e persiste os fornecedores válidos,
ignorando duplicatas e relatando erros linha-a-linha.

A geocodificação **não é feita aqui**: ``municipio_id``/``lat``/``lon``
ficam ``None`` e o cliente pode chamar ``POST /localizacoes/geocodificar``
em seguida para enriquecer os registros.

ADR-005: imports restritos a stdlib e :mod:`domain`.
"""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.portas.repositorios import RepositorioFornecedores

__all__ = [
    "ErroLinhaImportacao",
    "ImportarFornecedores",
    "LinhaImportacao",
    "ResultadoImportacao",
]


@dataclass(frozen=True)
class LinhaImportacao:
    """Uma linha crua do CSV/XLSX, já extraída pelo leitor."""

    nome: str
    cidade: str
    uf: str
    identificador_linha: int


@dataclass(frozen=True)
class ErroLinhaImportacao:
    """Relato de linha que não pode ser importada."""

    linha: int
    motivo: str


@dataclass(frozen=True)
class ResultadoImportacao:
    """Sumário agregado após processar o lote."""

    total_linhas: int
    importados: int
    duplicados: int
    erros: list[ErroLinhaImportacao]


class ImportarFornecedores:
    """Valida, deduplica e persiste fornecedores em lote."""

    def __init__(self, repositorio: RepositorioFornecedores) -> None:
        self._repo = repositorio

    async def executar(self, linhas: list[LinhaImportacao]) -> ResultadoImportacao:
        agora = utc_now()
        erros: list[ErroLinhaImportacao] = []
        duplicados = 0
        a_persistir: list[Fornecedor] = []
        ja_vistos: set[tuple[str, str, str]] = set()

        for linha in linhas:
            nome = linha.nome.strip()
            cidade = linha.cidade.strip()
            uf = linha.uf.strip().upper()

            if not nome:
                erros.append(
                    ErroLinhaImportacao(linha=linha.identificador_linha, motivo="nome vazio")
                )
                continue
            if not cidade:
                erros.append(
                    ErroLinhaImportacao(linha=linha.identificador_linha, motivo="cidade vazia")
                )
                continue
            if len(uf) != 2:
                erros.append(
                    ErroLinhaImportacao(
                        linha=linha.identificador_linha, motivo="uf inválida (esperado 2 letras)"
                    )
                )
                continue

            chave = (nome, cidade, uf)
            if chave in ja_vistos:
                duplicados += 1
                continue
            existente = await self._repo.buscar_por_nome_cidade_uf(nome, cidade, uf)
            if existente is not None:
                duplicados += 1
                ja_vistos.add(chave)
                continue

            ja_vistos.add(chave)
            a_persistir.append(
                Fornecedor(
                    id=gerar_id("forn"),
                    nome=nome,
                    cidade=cidade,
                    uf=uf,
                    criado_em=agora,
                    atualizado_em=agora,
                )
            )

        if a_persistir:
            await self._repo.salvar_lote(a_persistir)

        return ResultadoImportacao(
            total_linhas=len(linhas),
            importados=len(a_persistir),
            duplicados=duplicados,
            erros=erros,
        )
