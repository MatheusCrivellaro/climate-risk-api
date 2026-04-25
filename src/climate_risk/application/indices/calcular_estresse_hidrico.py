"""Caso de uso :class:`CalcularIndicesEstresseHidrico` (Slice 15 — lado síncrono).

Cria uma :class:`Execucao` ``pending`` do tipo ``"estresse_hidrico"`` e
enfileira um :class:`Job` ``"processar_estresse_hidrico"`` carregando os
três caminhos de arquivo e os limiares. O processamento assíncrono é
responsabilidade do handler/worker (ver
:mod:`climate_risk.application.jobs.handlers_estresse_hidrico`).

ADR-005: imports restritos a stdlib e :mod:`domain`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.calculos.estresse_hidrico import (
    ParametrosIndicesEstresseHidrico,
)
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.excecoes import ErroArquivoNCNaoEncontrado
from climate_risk.domain.portas.fila_jobs import FilaJobs
from climate_risk.domain.portas.repositorios import RepositorioExecucoes

__all__ = [
    "CalcularIndicesEstresseHidrico",
    "ExecucaoIniciada",
    "ParametrosCalculoEstresseHidrico",
    "ParametrosCalculoEstresseHidricoPasta",
]

TIPO_EXECUCAO = "estresse_hidrico"
TIPO_JOB = "processar_estresse_hidrico"
TIPO_JOB_PASTA = "processar_estresse_hidrico_pasta"
# ``variavel`` é obrigatória na entidade ``Execucao``; usamos um rótulo
# composto que identifica o trio (sem inventar um enum novo no domínio).
VARIAVEL_COMPOSTA = "pr+tas+evap"


@dataclass(frozen=True)
class ParametrosCalculoEstresseHidrico:
    """Entrada do caso de uso (agregada).

    Atributos:
        arquivo_pr: Caminho local do ``.nc`` de precipitação.
        arquivo_tas: Caminho local do ``.nc`` de temperatura do ar.
        arquivo_evap: Caminho local do ``.nc`` de evaporação.
        cenario: Rótulo CORDEX (ex.: ``"rcp45"``).
        parametros_indices: Limiares dos índices de estresse hídrico.
    """

    arquivo_pr: Path
    arquivo_tas: Path
    arquivo_evap: Path
    cenario: str
    parametros_indices: ParametrosIndicesEstresseHidrico


@dataclass(frozen=True)
class ParametrosCalculoEstresseHidricoPasta:
    """Entrada do caso de uso variante por pasta (Slice 17).

    Diferente de :class:`ParametrosCalculoEstresseHidrico`, recebe três
    diretórios — cada um deve conter um ou mais ``.nc`` que serão
    concatenados temporalmente pelo handler antes do cálculo.

    Atributos:
        pasta_pr: Diretório com os ``.nc`` de precipitação.
        pasta_tas: Diretório com os ``.nc`` de temperatura do ar.
        pasta_evap: Diretório com os ``.nc`` de evaporação.
        cenario: Rótulo CORDEX (ex.: ``"rcp45"``).
        parametros_indices: Limiares dos índices de estresse hídrico.
    """

    pasta_pr: Path
    pasta_tas: Path
    pasta_evap: Path
    cenario: str
    parametros_indices: ParametrosIndicesEstresseHidrico


@dataclass(frozen=True)
class ExecucaoIniciada:
    """Retorno do caso de uso, pronto para o response HTTP."""

    execucao_id: str
    job_id: str
    status: str
    criado_em: datetime


class CalcularIndicesEstresseHidrico:
    """Orquestra a criação da execução e o enfileiramento do job.

    Passos:

    1. Valida que os três arquivos existem; levanta
       :class:`ErroArquivoNCNaoEncontrado` caso contrário.
    2. Cria :class:`Execucao` com ``tipo='estresse_hidrico'`` e persiste.
    3. Enfileira o :class:`Job` ``processar_estresse_hidrico`` com payload
       JSON-serializável.
    4. Upsert da execução gravando o ``job_id``.
    """

    def __init__(
        self,
        repositorio_execucoes: RepositorioExecucoes,
        fila_jobs: FilaJobs,
    ) -> None:
        self._repo = repositorio_execucoes
        self._fila = fila_jobs

    async def executar(
        self,
        params: ParametrosCalculoEstresseHidrico,
    ) -> ExecucaoIniciada:
        for caminho in (params.arquivo_pr, params.arquivo_tas, params.arquivo_evap):
            if not caminho.exists():
                raise ErroArquivoNCNaoEncontrado(
                    caminho=str(caminho),
                    detalhe="arquivo não existe no filesystem.",
                )

        agora = utc_now()
        execucao_id = gerar_id("exec")
        execucao = Execucao(
            id=execucao_id,
            cenario=params.cenario,
            variavel=VARIAVEL_COMPOSTA,
            arquivo_origem=str(params.arquivo_pr),
            tipo=TIPO_EXECUCAO,
            parametros=_serializar_parametros(params),
            status=StatusExecucao.PENDING,
            criado_em=agora,
            concluido_em=None,
            job_id=None,
        )
        await self._repo.salvar(execucao)

        payload = _montar_payload(execucao_id, params)
        job = await self._fila.enfileirar(tipo=TIPO_JOB, payload=payload)

        execucao_com_job = Execucao(
            id=execucao.id,
            cenario=execucao.cenario,
            variavel=execucao.variavel,
            arquivo_origem=execucao.arquivo_origem,
            tipo=execucao.tipo,
            parametros=execucao.parametros,
            status=execucao.status,
            criado_em=execucao.criado_em,
            concluido_em=execucao.concluido_em,
            job_id=job.id,
        )
        await self._repo.salvar(execucao_com_job)

        return ExecucaoIniciada(
            execucao_id=execucao.id,
            job_id=job.id,
            status=execucao.status,
            criado_em=agora,
        )

    async def executar_de_pasta(
        self,
        params: ParametrosCalculoEstresseHidricoPasta,
    ) -> ExecucaoIniciada:
        """Variante por pasta (Slice 17): valida diretórios, cria execução, enfileira job.

        Cada pasta deve existir; o conteúdo (``.nc`` presentes/válidos) só é
        verificado pelo handler. Mesmo tipo de execução do caminho original
        (``"estresse_hidrico"``); o tipo de job é
        ``"processar_estresse_hidrico_pasta"`` para roteamento.
        """
        for caminho in (params.pasta_pr, params.pasta_tas, params.pasta_evap):
            if not caminho.exists() or not caminho.is_dir():
                raise ErroArquivoNCNaoEncontrado(
                    caminho=str(caminho),
                    detalhe="pasta não existe ou não é um diretório.",
                )

        agora = utc_now()
        execucao_id = gerar_id("exec")
        execucao = Execucao(
            id=execucao_id,
            cenario=params.cenario,
            variavel=VARIAVEL_COMPOSTA,
            arquivo_origem=str(params.pasta_pr),
            tipo=TIPO_EXECUCAO,
            parametros=_serializar_parametros_pasta(params),
            status=StatusExecucao.PENDING,
            criado_em=agora,
            concluido_em=None,
            job_id=None,
        )
        await self._repo.salvar(execucao)

        payload = _montar_payload_pasta(execucao_id, params)
        job = await self._fila.enfileirar(tipo=TIPO_JOB_PASTA, payload=payload)

        execucao_com_job = Execucao(
            id=execucao.id,
            cenario=execucao.cenario,
            variavel=execucao.variavel,
            arquivo_origem=execucao.arquivo_origem,
            tipo=execucao.tipo,
            parametros=execucao.parametros,
            status=execucao.status,
            criado_em=execucao.criado_em,
            concluido_em=execucao.concluido_em,
            job_id=job.id,
        )
        await self._repo.salvar(execucao_com_job)

        return ExecucaoIniciada(
            execucao_id=execucao.id,
            job_id=job.id,
            status=execucao.status,
            criado_em=agora,
        )


def _serializar_parametros(params: ParametrosCalculoEstresseHidrico) -> dict[str, Any]:
    return {
        "arquivo_pr": str(params.arquivo_pr),
        "arquivo_tas": str(params.arquivo_tas),
        "arquivo_evap": str(params.arquivo_evap),
        "limiar_pr_mm_dia": params.parametros_indices.limiar_pr_mm_dia,
        "limiar_tas_c": params.parametros_indices.limiar_tas_c,
    }


def _montar_payload(execucao_id: str, params: ParametrosCalculoEstresseHidrico) -> dict[str, Any]:
    return {
        "execucao_id": execucao_id,
        "arquivo_pr": str(params.arquivo_pr),
        "arquivo_tas": str(params.arquivo_tas),
        "arquivo_evap": str(params.arquivo_evap),
        "cenario": params.cenario,
        "limiar_pr_mm_dia": params.parametros_indices.limiar_pr_mm_dia,
        "limiar_tas_c": params.parametros_indices.limiar_tas_c,
    }


def _serializar_parametros_pasta(
    params: ParametrosCalculoEstresseHidricoPasta,
) -> dict[str, Any]:
    return {
        "pasta_pr": str(params.pasta_pr),
        "pasta_tas": str(params.pasta_tas),
        "pasta_evap": str(params.pasta_evap),
        "limiar_pr_mm_dia": params.parametros_indices.limiar_pr_mm_dia,
        "limiar_tas_c": params.parametros_indices.limiar_tas_c,
    }


def _montar_payload_pasta(
    execucao_id: str, params: ParametrosCalculoEstresseHidricoPasta
) -> dict[str, Any]:
    return {
        "execucao_id": execucao_id,
        "pasta_pr": str(params.pasta_pr),
        "pasta_tas": str(params.pasta_tas),
        "pasta_evap": str(params.pasta_evap),
        "cenario": params.cenario,
        "limiar_pr_mm_dia": params.parametros_indices.limiar_pr_mm_dia,
        "limiar_tas_c": params.parametros_indices.limiar_tas_c,
    }
