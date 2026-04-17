"""Entidade :class:`DadosClimaticos` — contrato de saída do leitor NetCDF."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class DadosClimaticos:
    """Dados climáticos normalizados vindos de um arquivo ``.nc``.

    Este é o contrato de retorno do adaptador de leitura NetCDF. Todos os
    campos são tipos primitivos ou arrays ``numpy`` — **sem vazar xarray,
    cftime ou qualquer outro tipo da camada ``infrastructure``** (ver
    ADR-005, seção "Regras de dependência").

    Atributos:
        dados_diarios: array 3D de forma ``(tempo, y, x)``, em ``mm/dia``.
        lat_2d: array 2D ``(y, x)`` com latitudes em graus decimais.
        lon_2d: array 2D ``(y, x)`` com longitudes normalizadas em
            ``[-180, 180]``.
        anos: array 1D (``dtype`` inteiro) com o ano de cada timestamp em
            ``dados_diarios`` — compatível com calendários ``cftime``.
        cenario: string normalizada em minúsculas (ex.: ``"rcp45"``,
            ``"ssp370"``, ``"unknown"``).
        variavel: nome da variável lida no dataset (ex.: ``"pr"``).
        unidade_original: string com a unidade bruta tal como encontrada
            em ``variavel.attrs["units"]`` antes da conversão.
        conversao_unidade_aplicada: ``True`` se houve multiplicação por
            ``86400`` (ou equivalente) para converter o fluxo para acumulado
            diário em ``mm/dia``. Ver ``ConversorPrecipitacao`` e ADR-007.
        calendario: string em minúsculas com o calendário do eixo temporal
            (ex.: ``"standard"``, ``"noleap"``, ``"360_day"``,
            ``"proleptic_gregorian"``).
        arquivo_origem: path absoluto ou relativo do ``.nc`` lido,
            preservado para auditoria/log.
    """

    dados_diarios: np.ndarray
    lat_2d: np.ndarray
    lon_2d: np.ndarray
    anos: np.ndarray
    cenario: str
    variavel: str
    unidade_original: str
    conversao_unidade_aplicada: bool
    calendario: str
    arquivo_origem: str
