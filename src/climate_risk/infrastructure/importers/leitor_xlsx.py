"""Leitor de XLSX (openpyxl) para import de fornecedores (Slice 10).

Formato esperado:

- Primeira aba ativa.
- Primeira linha = cabeçalhos (``nome``, ``cidade``, ``uf`` — aliases
  como ``razao_social`` e ``estado`` também são aceitos).
- Linhas subsequentes são fornecedores.

Linhas totalmente vazias são ignoradas. Células com tipos inesperados são
convertidas via ``str(...)`` — o caso de uso trata validação de conteúdo.
"""

from __future__ import annotations

import io
from typing import Any

import openpyxl

from climate_risk.infrastructure.importers.linha_bruta import LinhaImportacaoBruta

_ALIAS_NOME = ("nome", "razao_social", "razao social", "empresa")
_ALIAS_CIDADE = ("cidade", "municipio", "município")
_ALIAS_UF = ("uf", "estado", "sigla_uf")


def _mapear_colunas(cabecalhos: tuple[Any, ...]) -> dict[str, int]:
    normalizados = [(str(c).strip().lower() if c is not None else "") for c in cabecalhos]
    mapa: dict[str, int] = {}
    for idx, coluna in enumerate(normalizados):
        if "nome" not in mapa and coluna in _ALIAS_NOME:
            mapa["nome"] = idx
        elif "cidade" not in mapa and coluna in _ALIAS_CIDADE:
            mapa["cidade"] = idx
        elif "uf" not in mapa and coluna in _ALIAS_UF:
            mapa["uf"] = idx
    return mapa


def _celula_str(valor: Any) -> str:
    if valor is None:
        return ""
    return str(valor).strip()


def ler_fornecedores_xlsx(conteudo: bytes) -> list[LinhaImportacaoBruta]:
    """Lê a primeira aba do XLSX usando openpyxl em modo read-only."""
    workbook = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True, data_only=True)
    try:
        worksheet = workbook.active
        if worksheet is None:
            return []

        linhas_iter = worksheet.iter_rows(values_only=True)
        try:
            cabecalhos = next(linhas_iter)
        except StopIteration:
            return []

        mapa = _mapear_colunas(cabecalhos)
        if not {"nome", "cidade", "uf"}.issubset(mapa):
            return []

        resultado: list[LinhaImportacaoBruta] = []
        for numero_relativo, linha in enumerate(linhas_iter, start=2):
            if not linha or all(celula is None or str(celula).strip() == "" for celula in linha):
                continue
            resultado.append(
                LinhaImportacaoBruta(
                    nome=_celula_str(_pegar(linha, mapa["nome"])),
                    cidade=_celula_str(_pegar(linha, mapa["cidade"])),
                    uf=_celula_str(_pegar(linha, mapa["uf"])),
                    numero_linha=numero_relativo,
                )
            )
        return resultado
    finally:
        workbook.close()


def _pegar(linha: tuple[Any, ...], idx: int) -> Any:
    return linha[idx] if idx < len(linha) else None
