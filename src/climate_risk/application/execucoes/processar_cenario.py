"""Caso de uso :class:`ProcessarCenarioCordex` (UC-02 — lado assíncrono).

Executado **pelo Worker**, consumindo um :class:`Job` do tipo
``"processar_cordex"``. Orquestra:

1. Leitura do arquivo NetCDF.
2. Cálculo do P95 por célula (opcional).
3. Iteração por célula ``(iy, ix)`` dentro do bbox — **outer=iy,
   inner=ix** — preservando paridade numérica célula-a-célula com o
   script legado ``cordex_pr_freq_intensity.py`` (ver ADR-001; código
   legado removido na Slice 12 após validação bit-a-bit).
4. Cálculo dos índices anuais por (célula, ano).
5. Persistência em lotes de 1000 :class:`ResultadoIndice` via
   :class:`RepositorioResultados`.
6. Atualização final de :class:`Execucao` para status terminal
   (``completed`` ou ``failed``).

ADR-005: imports restritos a :mod:`stdlib`, :mod:`numpy` e :mod:`domain`.
Nenhum import de FastAPI, SQLAlchemy, Pydantic ou ``xarray``.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import numpy as np

from climate_risk.core.ids import gerar_id
from climate_risk.core.tempo import utc_now
from climate_risk.domain.entidades.execucao import Execucao, StatusExecucao
from climate_risk.domain.entidades.resultado import ResultadoIndice
from climate_risk.domain.espacial.bbox import BoundingBox, mascara_bbox
from climate_risk.domain.excecoes import ErroEntidadeNaoEncontrada
from climate_risk.domain.indices.calculadora import (
    IndicesAnuais,
    ParametrosIndices,
    calcular_indices_anuais,
)
from climate_risk.domain.indices.p95 import PeriodoBaseline, calcular_p95_por_celula_numpy
from climate_risk.domain.portas.leitor_netcdf import LeitorNetCDF
from climate_risk.domain.portas.repositorios import (
    RepositorioExecucoes,
    RepositorioResultados,
)

__all__ = [
    "UNIDADES_POR_INDICE",
    "ParametrosProcessamento",
    "ProcessarCenarioCordex",
    "ResultadoProcessamento",
]

logger = logging.getLogger(__name__)

# Idêntico ao dicionário de ``calcular_por_pontos``. Mantemos uma cópia
# aqui para evitar dependência cruzada entre submódulos de ``application``
# (cada caso de uso é autossuficiente).
UNIDADES_POR_INDICE: dict[str, str] = {
    "wet_days": "dias",
    "sdii": "mm/day",
    "rx1day": "mm",
    "rx5day": "mm",
    "r20mm": "dias",
    "r50mm": "dias",
    "r95ptot_mm": "mm",
    "r95ptot_frac": "adimensional",
}

# Tamanho do lote de persistência. 1000 balanceia latência de commit
# com pressao de memoria (~ 8 indices x 1000 linhas por transacao).
TAMANHO_LOTE_PERSISTENCIA = 1000


@dataclass(frozen=True)
class ParametrosProcessamento:
    """Entrada do caso de uso assíncrono.

    Todos os campos derivam do payload JSON do :class:`Job`; a conversão
    acontece no handler (ver :mod:`climate_risk.application.jobs.handlers_cordex`).
    """

    execucao_id: str
    arquivo_nc: str
    variavel: str
    bbox: BoundingBox | None
    parametros_indices: ParametrosIndices
    p95_baseline: PeriodoBaseline | None
    p95_wet_thr: float


@dataclass(frozen=True)
class ResultadoProcessamento:
    """Sumário devolvido ao handler para log/observabilidade."""

    execucao_id: str
    total_celulas: int
    total_anos: int
    total_resultados: int


class ProcessarCenarioCordex:
    """Processa um cenário CORDEX completo, persistindo :class:`ResultadoIndice`.

    Transições de :class:`Execucao`:

    - entrada: ``pending`` (criada pelo :class:`CriarExecucaoCordex`).
    - início: transiciona para ``running``.
    - fim bem-sucedido: ``completed`` com ``concluido_em`` preenchido.
    - fim com falha: ``failed`` + ``concluido_em``. A exceção é re-levantada
      para que o Worker decida sobre o retry (a fila registra o ``erro`` no
      Job; a Execucao mantém apenas status terminal).
    """

    def __init__(
        self,
        leitor_netcdf: LeitorNetCDF,
        repositorio_execucoes: RepositorioExecucoes,
        repositorio_resultados: RepositorioResultados,
    ) -> None:
        self._leitor = leitor_netcdf
        self._repo_execucoes = repositorio_execucoes
        self._repo_resultados = repositorio_resultados

    async def executar(self, params: ParametrosProcessamento) -> ResultadoProcessamento:
        """Executa o processamento; ver docstring da classe."""
        execucao = await self._carregar_execucao(params.execucao_id)
        execucao = await self._transicionar(execucao, StatusExecucao.RUNNING, concluido=False)

        try:
            sumario = await self._processar(execucao, params)
        except Exception:
            await self._transicionar(execucao, StatusExecucao.FAILED, concluido=True)
            raise

        await self._transicionar(execucao, StatusExecucao.COMPLETED, concluido=True)
        return sumario

    async def _processar(
        self, execucao: Execucao, params: ParametrosProcessamento
    ) -> ResultadoProcessamento:
        dados = await self._leitor.abrir(params.arquivo_nc, params.variavel)

        p95_grid = calcular_p95_por_celula_numpy(
            dados_diarios=dados.dados_diarios,
            anos_por_dia=dados.anos,
            baseline=params.p95_baseline,
            p95_wet_thr=params.p95_wet_thr,
        )

        anos_unicos = sorted({int(a) for a in np.asarray(dados.anos).tolist()})
        mascara_celulas = _mascara_celulas(dados.lat_2d, dados.lon_2d, params.bbox)

        lote: list[ResultadoIndice] = []
        total_celulas = 0
        total_resultados = 0

        ny, nx = dados.lat_2d.shape
        for iy in range(ny):
            for ix in range(nx):
                if not mascara_celulas[iy, ix]:
                    continue
                total_celulas += 1
                lat_celula = float(dados.lat_2d[iy, ix])
                lon_celula = float(dados.lon_2d[iy, ix])
                serie_pixel = np.asarray(dados.dados_diarios[:, iy, ix])
                p95_thr = _p95_para_pixel(p95_grid, iy, ix)

                for ano in anos_unicos:
                    mascara_ano = dados.anos == ano
                    if not bool(np.any(mascara_ano)):
                        continue
                    serie_anual = serie_pixel[mascara_ano]
                    indices = calcular_indices_anuais(
                        serie_anual, params.parametros_indices, p95_thr=p95_thr
                    )
                    for nome, valor in _achatar_indices(indices).items():
                        lote.append(
                            ResultadoIndice(
                                id=gerar_id("res"),
                                execucao_id=execucao.id,
                                lat=lat_celula,
                                lon=lon_celula,
                                lat_input=None,
                                lon_input=None,
                                ano=ano,
                                nome_indice=nome,
                                valor=_nan_para_none(valor),
                                unidade=UNIDADES_POR_INDICE[nome],
                                municipio_id=None,
                            )
                        )
                        total_resultados += 1
                    if len(lote) >= TAMANHO_LOTE_PERSISTENCIA:
                        await self._repo_resultados.salvar_lote(lote)
                        lote = []

        if lote:
            await self._repo_resultados.salvar_lote(lote)

        logger.info(
            "ProcessarCenarioCordex concluído execucao_id=%s celulas=%d anos=%d linhas=%d",
            execucao.id,
            total_celulas,
            len(anos_unicos),
            total_resultados,
        )
        return ResultadoProcessamento(
            execucao_id=execucao.id,
            total_celulas=total_celulas,
            total_anos=len(anos_unicos),
            total_resultados=total_resultados,
        )

    async def _carregar_execucao(self, execucao_id: str) -> Execucao:
        execucao = await self._repo_execucoes.buscar_por_id(execucao_id)
        if execucao is None:
            raise ErroEntidadeNaoEncontrada(entidade="Execucao", identificador=execucao_id)
        return execucao

    async def _transicionar(
        self, execucao: Execucao, novo_status: str, *, concluido: bool
    ) -> Execucao:
        agora = utc_now()
        atualizada = Execucao(
            id=execucao.id,
            cenario=execucao.cenario,
            variavel=execucao.variavel,
            arquivo_origem=execucao.arquivo_origem,
            tipo=execucao.tipo,
            parametros=execucao.parametros,
            status=novo_status,
            criado_em=execucao.criado_em,
            concluido_em=agora if concluido else execucao.concluido_em,
            job_id=execucao.job_id,
        )
        await self._repo_execucoes.salvar(atualizada)
        return atualizada


def _mascara_celulas(
    lat_2d: np.ndarray, lon_2d: np.ndarray, bbox: BoundingBox | None
) -> np.ndarray:
    """Retorna máscara 2D ``(y, x)`` — tudo ``True`` se ``bbox is None``."""
    if bbox is None:
        return np.ones(lat_2d.shape, dtype=bool)
    ny, nx = lat_2d.shape
    return np.asarray(mascara_bbox(lat_2d, lon_2d, bbox)).reshape(ny, nx)


def _p95_para_pixel(p95_grid: np.ndarray | None, iy: int, ix: int) -> float | None:
    if p95_grid is None:
        return None
    valor = float(p95_grid[iy, ix])
    return valor if math.isfinite(valor) else None


def _achatar_indices(indices: IndicesAnuais) -> dict[str, float]:
    return {
        "wet_days": float(indices.wet_days),
        "sdii": indices.sdii,
        "rx1day": indices.rx1day,
        "rx5day": indices.rx5day,
        "r20mm": float(indices.r20mm),
        "r50mm": float(indices.r50mm),
        "r95ptot_mm": indices.r95ptot_mm,
        "r95ptot_frac": indices.r95ptot_frac,
    }


def _nan_para_none(valor: float) -> float | None:
    return None if math.isnan(valor) else float(valor)
