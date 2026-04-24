"""Entidade :class:`DadosClimaticosMultiVariaveis` — saída do leitor multi-variável.

Contrato de retorno do adaptador de leitura multi-variável do Slice 13. Cada
variável **mantém a sua grade original** — a agregação para uma grade comum
(município) fica na próxima slice. A única exigência sobre as três
``DataArray`` é compartilharem o **mesmo eixo temporal**, que fica
explicitado em :attr:`tempo` (já convertido para calendário gregoriano).

A exposição de ``xr.DataArray`` na camada de domínio é uma exceção consciente
à regra geral de ADR-005 ("domínio sem xarray"): preservar a grade nativa de
cada fonte é caro de representar só com ``numpy`` (coords 1Dx1D vs 2Dx2D em
*rotated pole*), e a alternativa — trafegar três tuplas ``(valores, lat, lon)``
separadas por variável — complica a leitura sem benefício arquitetural real.
Ver ADR-009 para o contexto completo.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import xarray as xr


@dataclass(frozen=True)
class DadosClimaticosMultiVariaveis:
    """Lote de três variáveis CORDEX alinhadas temporalmente.

    Atributos:
        precipitacao_diaria_mm: ``DataArray`` de precipitação em ``mm/dia``,
            grade preservada da fonte original (tipicamente INPE-Eta 1D).
        temperatura_diaria_c: ``DataArray`` de temperatura do ar em ``°C``,
            mesma grade da precipitação no caso atual, mas isso **não** é
            garantido pelo tipo — é responsabilidade do caso de uso validar
            se quiser assumir co-registro.
        evaporacao_diaria_mm: ``DataArray`` de evaporação em ``mm/dia``,
            tipicamente em grade *rotated pole* 2D (SMHI-RCA4) — incompatível
            com a grade de ``pr``/``tas``.
        tempo: :class:`pandas.DatetimeIndex` com os timestamps comuns às três
            séries (interseção). Sempre em calendário gregoriano; arquivos
            ``noleap`` ou ``360_day`` já foram convertidos no adapter.
        cenario: Cenário CORDEX compartilhado pelos três arquivos
            (ex.: ``"rcp45"``, ``"rcp85"``). Sempre em minúsculas.
    """

    precipitacao_diaria_mm: xr.DataArray
    temperatura_diaria_c: xr.DataArray
    evaporacao_diaria_mm: xr.DataArray
    tempo: pd.DatetimeIndex
    cenario: str

    def validar(self) -> None:
        """Confere que as três ``DataArray`` compartilham o eixo temporal.

        Não valida grade espacial — grades distintas são esperadas e suportadas
        (serão resolvidas por agregação em município na próxima slice). Só o
        eixo temporal precisa coincidir, porque os cálculos anuais assumem
        uma janela comum dia-a-dia.

        Raises:
            ValueError: quando alguma ``DataArray`` tem dimensão ``time``
                diferente de :attr:`tempo`, ou quando a dimensão ``time`` está
                ausente em qualquer uma das três.
        """
        esperado = len(self.tempo)
        for nome, da in (
            ("precipitacao_diaria_mm", self.precipitacao_diaria_mm),
            ("temperatura_diaria_c", self.temperatura_diaria_c),
            ("evaporacao_diaria_mm", self.evaporacao_diaria_mm),
        ):
            if "time" not in da.dims:
                raise ValueError(f"'{nome}' não possui dimensão 'time'.")
            if da.sizes["time"] != esperado:
                raise ValueError(
                    f"'{nome}' tem {da.sizes['time']} passos temporais; "
                    f"esperado {esperado} (conforme 'tempo')."
                )
