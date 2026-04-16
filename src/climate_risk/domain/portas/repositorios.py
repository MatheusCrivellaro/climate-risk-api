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


class RepositorioMunicipios(Protocol):
    """Cache local de municípios do IBGE."""

    async def buscar_por_id(self, municipio_id: int) -> Municipio | None: ...

    async def buscar_por_nome_uf(self, nome_normalizado: str, uf: str) -> Municipio | None: ...

    async def salvar(self, municipio: Municipio) -> None:
        """Insere ou atualiza o município (upsert por ``id``)."""
        ...

    async def listar(
        self,
        uf: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Municipio]: ...

    async def contar(self, uf: str | None = None) -> int: ...


class RepositorioFornecedores(Protocol):
    """CRUD de fornecedores geolocalizados."""

    async def buscar_por_id(self, fornecedor_id: str) -> Fornecedor | None: ...

    async def salvar(self, fornecedor: Fornecedor) -> None:
        """Insere o fornecedor. Levanta :class:`ErroConflito` se ``id`` já existe."""
        ...

    async def listar(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Fornecedor]: ...

    async def contar(self) -> int: ...

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
