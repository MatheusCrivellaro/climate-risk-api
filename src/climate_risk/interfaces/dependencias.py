"""Dependências compartilhadas do FastAPI.

Expõe providers prontos para ``Depends(...)`` que injetam adaptadores de
``infrastructure`` nos casos de uso de ``application``. Todas as portas
ficam do lado de ``domain``; aqui apenas compomos.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from climate_risk.application.calculos.calcular_por_pontos import CalcularIndicesPorPontos
from climate_risk.application.calculos.criar_execucao_por_pontos import CriarExecucaoPorPontos
from climate_risk.application.cobertura import AnalisarCoberturaFornecedores
from climate_risk.application.execucoes.cancelar import CancelarExecucao
from climate_risk.application.execucoes.consultar import ConsultarExecucoes
from climate_risk.application.execucoes.criar import CriarExecucaoCordex
from climate_risk.application.fornecedores import (
    ConsultarFornecedores,
    CriarFornecedor,
    ImportarFornecedores,
    RemoverFornecedor,
)
from climate_risk.application.geocodificacao import (
    GeocodificarLocalizacoes,
    RefreshCatalogoIBGE,
)
from climate_risk.application.jobs.consultar import ConsultarJobs
from climate_risk.application.jobs.reprocessar import ReprocessarJob
from climate_risk.application.localizacoes import LocalizarPontos
from climate_risk.application.resultados import (
    AgregarResultados,
    ConsultarResultados,
    ConsultarStats,
)
from climate_risk.core.config import Settings, get_settings
from climate_risk.domain.portas.shapefile_municipios import ShapefileMunicipios
from climate_risk.infrastructure.db.repositorios.execucoes import (
    SQLAlchemyRepositorioExecucoes,
)
from climate_risk.infrastructure.db.repositorios.fornecedores import (
    SQLAlchemyRepositorioFornecedores,
)
from climate_risk.infrastructure.db.repositorios.jobs import SQLAlchemyRepositorioJobs
from climate_risk.infrastructure.db.repositorios.municipios import (
    SQLAlchemyRepositorioMunicipios,
)
from climate_risk.infrastructure.db.repositorios.resultados import (
    SQLAlchemyRepositorioResultados,
)
from climate_risk.infrastructure.db.sessao import get_sessao
from climate_risk.infrastructure.fila.fila_sqlite import FilaSQLite
from climate_risk.infrastructure.geocodificacao import CalculadorShapely, ClienteIBGEHttp
from climate_risk.infrastructure.netcdf.leitor_xarray import LeitorXarray
from climate_risk.infrastructure.shapefile import ShapefileGeopandas


def obter_settings() -> Settings:
    """Adapter para ``FastAPI.Depends`` que devolve a configuração global."""
    return get_settings()


def obter_leitor_netcdf() -> LeitorXarray:
    """Instancia o adaptador :class:`LeitorXarray` por request.

    A classe é leve (sem estado mutável relevante) — criar um por request
    evita compartilhar ``FileHandle`` abertos entre corrotinas.
    """
    return LeitorXarray()


SessaoDep = Annotated[AsyncSession, Depends(get_sessao)]


def obter_repositorio_execucoes(sessao: SessaoDep) -> SQLAlchemyRepositorioExecucoes:
    """Repositório de execuções ligado à sessão da request."""
    return SQLAlchemyRepositorioExecucoes(sessao)


def obter_repositorio_resultados(sessao: SessaoDep) -> SQLAlchemyRepositorioResultados:
    """Repositório de resultados ligado à sessão da request."""
    return SQLAlchemyRepositorioResultados(sessao)


def obter_repositorio_jobs(sessao: SessaoDep) -> SQLAlchemyRepositorioJobs:
    """Repositório CRUD de jobs ligado à sessão da request."""
    return SQLAlchemyRepositorioJobs(sessao)


def obter_fila_jobs(sessao: SessaoDep) -> FilaSQLite:
    """Fila de jobs ligada à sessão da request."""
    return FilaSQLite(sessao)


def obter_repositorio_municipios(sessao: SessaoDep) -> SQLAlchemyRepositorioMunicipios:
    """Repositório de municípios ligado à sessão da request."""
    return SQLAlchemyRepositorioMunicipios(sessao)


def obter_repositorio_fornecedores(sessao: SessaoDep) -> SQLAlchemyRepositorioFornecedores:
    """Repositório de fornecedores ligado à sessão da request (Slice 10)."""
    return SQLAlchemyRepositorioFornecedores(sessao)


def obter_cliente_ibge() -> ClienteIBGEHttp:
    """Instancia :class:`ClienteIBGEHttp` — lê settings via ``get_settings``."""
    return ClienteIBGEHttp()


def obter_calculador_centroide() -> CalculadorShapely:
    """Instancia :class:`CalculadorShapely` (puro, sem estado)."""
    return CalculadorShapely()


LeitorNetCDFDep = Annotated[LeitorXarray, Depends(obter_leitor_netcdf)]
RepoExecucoesDep = Annotated[SQLAlchemyRepositorioExecucoes, Depends(obter_repositorio_execucoes)]
RepoResultadosDep = Annotated[
    SQLAlchemyRepositorioResultados, Depends(obter_repositorio_resultados)
]
RepoJobsDep = Annotated[SQLAlchemyRepositorioJobs, Depends(obter_repositorio_jobs)]
FilaJobsDep = Annotated[FilaSQLite, Depends(obter_fila_jobs)]
RepoMunicipiosDep = Annotated[
    SQLAlchemyRepositorioMunicipios, Depends(obter_repositorio_municipios)
]
RepoFornecedoresDep = Annotated[
    SQLAlchemyRepositorioFornecedores, Depends(obter_repositorio_fornecedores)
]
ClienteIBGEDep = Annotated[ClienteIBGEHttp, Depends(obter_cliente_ibge)]
CalculadorCentroideDep = Annotated[CalculadorShapely, Depends(obter_calculador_centroide)]


def obter_caso_uso_calcular_por_pontos(
    leitor: LeitorNetCDFDep,
) -> CalcularIndicesPorPontos:
    """Compõe :class:`CalcularIndicesPorPontos` — caso de uso puro (Slice 4)."""
    return CalcularIndicesPorPontos(leitor_netcdf=leitor)


def obter_caso_uso_criar_execucao(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CriarExecucaoCordex:
    """Compõe :class:`CriarExecucaoCordex` com repositório + fila."""
    return CriarExecucaoCordex(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_caso_uso_criar_execucao_por_pontos(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CriarExecucaoPorPontos:
    """Compõe :class:`CriarExecucaoPorPontos` (UC-03 assíncrono — Slice 7)."""
    return CriarExecucaoPorPontos(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_consultar_execucoes(repo_execucoes: RepoExecucoesDep) -> ConsultarExecucoes:
    """Compõe :class:`ConsultarExecucoes` sobre o repositório."""
    return ConsultarExecucoes(repositorio=repo_execucoes)


def obter_caso_uso_cancelar_execucao(
    repo_execucoes: RepoExecucoesDep,
    fila: FilaJobsDep,
) -> CancelarExecucao:
    """Compõe :class:`CancelarExecucao` — cancela execução + job vinculado."""
    return CancelarExecucao(repositorio_execucoes=repo_execucoes, fila_jobs=fila)


def obter_consultar_jobs(repo_jobs: RepoJobsDep) -> ConsultarJobs:
    """Compõe :class:`ConsultarJobs` sobre o repositório CRUD."""
    return ConsultarJobs(repositorio=repo_jobs)


def obter_reprocessar_job(repo_jobs: RepoJobsDep) -> ReprocessarJob:
    """Compõe :class:`ReprocessarJob` sobre o repositório CRUD."""
    return ReprocessarJob(repositorio=repo_jobs)


def obter_caso_uso_geocodificar(
    repo_municipios: RepoMunicipiosDep,
    cliente: ClienteIBGEDep,
    centroide: CalculadorCentroideDep,
) -> GeocodificarLocalizacoes:
    """Compõe :class:`GeocodificarLocalizacoes` (UC-04 — Slice 8)."""
    return GeocodificarLocalizacoes(
        repositorio_municipios=repo_municipios,
        cliente_ibge=cliente,
        calculador_centroide=centroide,
    )


def obter_caso_uso_refresh_ibge(
    repo_municipios: RepoMunicipiosDep,
    cliente: ClienteIBGEDep,
    centroide: CalculadorCentroideDep,
) -> RefreshCatalogoIBGE:
    """Compõe :class:`RefreshCatalogoIBGE` (``POST /admin/ibge/refresh``)."""
    return RefreshCatalogoIBGE(
        repositorio_municipios=repo_municipios,
        cliente_ibge=cliente,
        calculador_centroide=centroide,
    )


@lru_cache(maxsize=1)
def _carregar_shapefile_cacheado(caminho: str) -> ShapefileGeopandas:
    """Carrega o shapefile uma única vez por processo.

    O :class:`ShapefileGeopandas` mantém o ``GeoDataFrame`` em memória
    (~50 MB para a malha completa), então instanciamos no máximo uma vez.
    A chave é o ``caminho`` — trocar o path invalida a cache (relevante
    em testes que usam ``app.dependency_overrides``).
    """
    return ShapefileGeopandas(caminho)


def obter_shapefile() -> ShapefileMunicipios:
    """Provider do singleton :class:`ShapefileGeopandas` (Slice 9).

    Lê ``shapefile_mun_path`` em ``Settings``; levanta
    :class:`ErroConfiguracao` se o path estiver vazio ou o arquivo não
    existir — o middleware HTTP mapeia para 500 com Problem Details.
    """
    settings = get_settings()
    return _carregar_shapefile_cacheado(settings.shapefile_mun_path or "")


ShapefileDep = Annotated[ShapefileMunicipios, Depends(obter_shapefile)]


def obter_caso_uso_localizar_pontos(shapefile: ShapefileDep) -> LocalizarPontos:
    """Compõe :class:`LocalizarPontos` (Slice 9)."""
    return LocalizarPontos(shapefile=shapefile)


def obter_caso_uso_analisar_cobertura(
    repo_municipios: RepoMunicipiosDep,
    cliente: ClienteIBGEDep,
    centroide: CalculadorCentroideDep,
    repo_resultados: RepoResultadosDep,
) -> AnalisarCoberturaFornecedores:
    """Compõe :class:`AnalisarCoberturaFornecedores` reusando Slice 8."""
    geocodificar = GeocodificarLocalizacoes(
        repositorio_municipios=repo_municipios,
        cliente_ibge=cliente,
        calculador_centroide=centroide,
    )
    return AnalisarCoberturaFornecedores(
        geocodificar=geocodificar,
        repositorio_resultados=repo_resultados,
    )


def obter_caso_uso_criar_fornecedor(repo: RepoFornecedoresDep) -> CriarFornecedor:
    """Compõe :class:`CriarFornecedor` (Slice 10)."""
    return CriarFornecedor(repositorio=repo)


def obter_caso_uso_consultar_fornecedores(repo: RepoFornecedoresDep) -> ConsultarFornecedores:
    """Compõe :class:`ConsultarFornecedores` (Slice 10)."""
    return ConsultarFornecedores(repositorio=repo)


def obter_caso_uso_remover_fornecedor(repo: RepoFornecedoresDep) -> RemoverFornecedor:
    """Compõe :class:`RemoverFornecedor` (Slice 10)."""
    return RemoverFornecedor(repositorio=repo)


def obter_caso_uso_importar_fornecedores(repo: RepoFornecedoresDep) -> ImportarFornecedores:
    """Compõe :class:`ImportarFornecedores` (Slice 10)."""
    return ImportarFornecedores(repositorio=repo)


def obter_caso_uso_consultar_resultados(repo: RepoResultadosDep) -> ConsultarResultados:
    """Compõe :class:`ConsultarResultados` (Slice 11)."""
    return ConsultarResultados(repositorio=repo)


def obter_caso_uso_agregar_resultados(repo: RepoResultadosDep) -> AgregarResultados:
    """Compõe :class:`AgregarResultados` (Slice 11)."""
    return AgregarResultados(repositorio=repo)


def obter_caso_uso_consultar_stats(repo: RepoResultadosDep) -> ConsultarStats:
    """Compõe :class:`ConsultarStats` (Slice 11)."""
    return ConsultarStats(repositorio=repo)
