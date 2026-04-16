"""Entidade ResultadoIndice.

Uma linha por (execução, célula, ano, índice) — formato longo que facilita
consultas filtradas e agregações sem duplicação de colunas por índice.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResultadoIndice:
    """Resultado pontual de um índice climático em um ano.

    Atributos:
        id: ULID com prefixo ``"res_"``.
        execucao_id: FK para :class:`Execucao`.
        lat: Latitude da célula do grid após snap (graus decimais).
        lon: Longitude da célula do grid após snap (graus decimais,
            ``-180..180``).
        lat_input: Latitude original enviada em UC-03 antes do snap. ``None``
            para resultados de UC-02 (grade).
        lon_input: Idem para longitude.
        ano: Ano do resultado (ex.: ``2030``).
        nome_indice: Código do índice (``"wet_days"``, ``"sdii"``,
            ``"rx1day"``, ``"rx5day"``, ``"r20mm"``, ``"r50mm"``,
            ``"r95ptot_mm"``, ``"r95ptot_frac"``).
        valor: Valor numérico ou ``None`` quando o cálculo resultou em
            ``NaN`` no domínio (ex.: série vazia).
        unidade: Unidade do valor (ex.: ``"mm"``, ``"dias"``, ``"-"``).
        municipio_id: Código IBGE do município enriquecido; ``None`` quando
            ainda não geocodificado ou fora do território.
    """

    id: str
    execucao_id: str
    lat: float
    lon: float
    lat_input: float | None
    lon_input: float | None
    ano: int
    nome_indice: str
    valor: float | None
    unidade: str
    municipio_id: int | None
