"""Portas (``typing.Protocol``) dos repositórios.

Cada :class:`Protocol` descreve o contrato que uma implementação concreta
(camada ``infrastructure``) deve satisfazer. Usamos ``Protocol`` — e não
``ABC`` — para permitir duck typing: qualquer classe com as assinaturas
corretas satisfaz o contrato sem herança explícita. Isso mantém
``infrastructure`` livre de imports de ``domain`` além dos tipos de dado.

Todas as operações são ``async`` pois o driver de banco é ``aiosqlite`` /
``asyncpg``. Nenhuma assinatura usa ``Any`` em retorno — os tipos são as
próprias entidades de domínio.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from climate_risk.domain.entidades import (
    Execucao,
    Fornecedor,
    Job,
    Municipio,
    ResultadoIndice,
)
from climate_risk.domain.espacial.bbox import BoundingBox
from climate_risk.domain.portas.filtros_resultados import (
    FiltrosAgregacaoResultados,
    FiltrosConsultaResultados,
    GrupoAgregadoRaw,
)


class RepositorioMunicipios(Protocol):
    """Cache local de municípios do IBGE."""

    async def buscar_por_id(self, municipio_id: int) -> Municipio | None: ...

    async def buscar_por_nome_uf(self, nome_normalizado: str, uf: str) -> Municipio | None: ...

    async def listar_por_uf(self, uf: str) -> list[Municipio]:
        """Todos os municípios de uma UF (ordenado por nome normalizado).

        Usado pelo *fuzzy match*: o caso de uso carrega o candidato-set uma
        única vez por UF e alimenta o ``rapidfuzz.process.extractOne``.
        """
        ...

    async def salvar(self, municipio: Municipio) -> None:
        """Insere ou atualiza o município (upsert por ``id``)."""
        ...

    async def salvar_lote(self, municipios: Sequence[Municipio]) -> None:
        """Upsert em massa (``POST /admin/ibge/refresh`` insere ~5570 linhas)."""
        ...

    async def listar(
        self,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Municipio]: ...

    async def contar(self, uf: str | None = None) -> int: ...


class RepositorioFornecedores(Protocol):
    """CRUD de fornecedores."""

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor | None: ...

    async def buscar_por_nome_cidade_uf(self, nome: str, cidade: str, uf: str) -> Fornecedor | None:
        """Usado pelo import para detectar duplicatas (combinação lógica única)."""
        ...

    async def salvar(self, fornecedor: Fornecedor) -> None:
        """Insere o fornecedor. Levanta :class:`ErroConflito` se ``id`` já existe."""
        ...

    async def salvar_lote(self, fornecedores: Sequence[Fornecedor]) -> None:
        """Insere vários fornecedores em uma transação."""
        ...

    async def listar(
        self,
        uf: str | None = None,
        cidade: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Fornecedor]: ...

    async def contar(
        self,
        uf: str | None = None,
        cidade: str | None = None,
    ) -> int: ...

    async def remover(self, fornecedor_id: str) -> bool:
        """Remove e retorna ``True`` se o fornecedor existia; ``False`` caso contrário."""
        ...


class RepositorioExecucoes(Protocol):
    """CRUD de execuções."""

    async def buscar_por_id(self, execucao_id: str) -> Execucao | None: ...

    async def salvar(self, execucao: Execucao) -> None:
        """Insere ou atualiza a execução (upsert por ``id``)."""
        ...

    async def listar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Execucao]: ...

    async def contar(
        self,
        cenario: str | None = None,
        variavel: str | None = None,
        status: str | None = None,
    ) -> int: ...


class RepositorioResultados(Protocol):
    """Persistência em lote e consulta rica de :class:`ResultadoIndice`."""

    async def salvar_lote(self, resultados: Sequence[ResultadoIndice]) -> None: ...

    async def listar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]: ...

    async def contar(
        self,
        execucao_id: str | None = None,
        cenario: str | None = None,
        variavel: str | None = None,
        ano_min: int | None = None,
        ano_max: int | None = None,
        nome_indice: str | None = None,
        bbox: BoundingBox | None = None,
        uf: str | None = None,
        municipio_id: int | None = None,
    ) -> int: ...

    async def municipios_com_resultados(self, municipios_ids: set[int]) -> set[int]:
        """Retorna o subconjunto de IDs com ao menos um :class:`ResultadoIndice`.

        Usado por :class:`AnalisarCoberturaFornecedores` (Slice 9) para
        identificar quais municípios geocodificados de fornecedores já
        estão cobertos pela grade CORDEX processada.

        Resultados com ``municipio_id IS NULL`` (processados por BBOX sem
        geocodificação) são ignorados — só entram linhas com ID explícito.
        """
        ...

    async def consultar(
        self,
        filtros: FiltrosConsultaResultados,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ResultadoIndice]:
        """Retorna resultados aplicando o conjunto rico de filtros (Slice 11).

        Diferente de :meth:`listar` (kwargs legados do Slice 9), aceita um
        DTO frozen que carrega também ``ano`` exato, ``nomes_indices`` em
        ``IN(...)`` e BBOX com cruzamento de antimeridiano.
        """
        ...

    async def contar_por_filtros(self, filtros: FiltrosConsultaResultados) -> int:
        """``COUNT(*)`` sob as mesmas condições de :meth:`consultar`."""
        ...

    async def agregar(
        self, filtros_agregacao: FiltrosAgregacaoResultados
    ) -> list[GrupoAgregadoRaw]:
        """Aplica ``GROUP BY`` + função de agregação sobre ``valor``.

        Percentis (``"p50"``, ``"p95"``) são calculados em Python (SQLite
        não oferece ``PERCENTILE_CONT``). Os demais (``media``, ``min``,
        ``max``, ``count``) são resolvidos em SQL.
        """
        ...

    async def distinct_cenarios(self) -> list[str]:
        """Cenários distintos presentes em execuções com resultados."""
        ...

    async def distinct_anos(self) -> list[int]:
        """Anos distintos em ``resultado_indice.ano`` (ordem crescente)."""
        ...

    async def distinct_variaveis(self) -> list[str]:
        """Variáveis climáticas distintas em execuções com resultados."""
        ...

    async def distinct_nomes_indices(self) -> list[str]:
        """Nomes de índice distintos em ``resultado_indice.nome_indice``."""
        ...

    async def contar_execucoes_com_resultados(self) -> int:
        """Quantidade de execuções distintas que produziram ao menos um resultado."""
        ...

    async def contar_resultados(self) -> int:
        """``COUNT(*)`` total de ``resultado_indice`` (sem filtros)."""
        ...


class RepositorioJobs(Protocol):
    """CRUD de :class:`Job`. A lógica de fila (aquire, heartbeat, retry) virá no Slice 5."""

    async def buscar_por_id(self, job_id: str) -> Job | None: ...

    async def salvar(self, job: Job) -> None:
        """Insere ou atualiza o job (upsert por ``id``)."""
        ...

    async def listar(
        self,
        status: str | None = None,
        tipo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]: ...

    async def contar(
        self,
        status: str | None = None,
        tipo: str | None = None,
    ) -> int: ...
