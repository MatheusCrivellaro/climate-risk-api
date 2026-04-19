"""Caso de uso :class:`CriarFornecedor` (Slice 10).

Registra um fornecedor a partir de ``(nome, cidade, uf)``. ``municipio_id``
e coordenadas são opcionais — o fluxo de geocodificação é chamado em
seguida via ``POST /localizacoes/geocodificar`` e atualiza o registro.

ADR-005: imports restritos a stdlib e :mod:`domain`.
"""

from __future__ import annotations

from dataclasses import dataclass

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.fornecedor import Fornecedor
from climate_risk.domain.portas.repositorios import RepositorioFornecedores

__all__ = ["CriarFornecedor", "ParametrosCriacaoFornecedor"]


@dataclass(frozen=True)
class ParametrosCriacaoFornecedor:
    """Entrada do caso de uso."""

    nome: str
    cidade: str
    uf: str
    identificador_externo: str | None = None
    municipio_id: int | None = None


class CriarFornecedor:
    """Gera ID, preenche timestamps, persiste e devolve a entidade."""

    def __init__(self, repositorio: RepositorioFornecedores) -> None:
        self._repo = repositorio

    async def executar(self, params: ParametrosCriacaoFornecedor) -> Fornecedor:
        agora = utc_now()
        fornecedor = Fornecedor(
            id=gerar_id("forn"),
            nome=params.nome,
            cidade=params.cidade,
            uf=params.uf,
            criado_em=agora,
            atualizado_em=agora,
            identificador_externo=params.identificador_externo,
            municipio_id=params.municipio_id,
        )
        await self._repo.salvar(fornecedor)
        return fornecedor
