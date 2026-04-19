"""Leitor de CSV para import de fornecedores (Slice 10).

Converte bytes em ``list[LinhaImportacaoBruta]``. Linhas mal formadas ou
com colunas obrigatórias vazias voltam com campos vazios — o caso de uso
:class:`ImportarFornecedores` é quem decide se aceita ou relata erro.

Heurísticas suportadas:

- Encoding: tenta UTF-8 (com BOM) primeiro; cai para latin-1 em falha.
- Separador: usa ``csv.Sniffer`` restrito a ``","`` e ``";"``; se a
  detecção falhar, usa vírgula.
- Cabeçalhos case-insensitive; aceita ``nome``, ``cidade``, ``uf``
  (e também ``estado`` como alias de ``uf``, para compat com planilhas
  legadas em que a sigla aparecia na coluna "Estado").
"""

from __future__ import annotations

import csv
import io

from climate_risk.infrastructure.importers.linha_bruta import LinhaImportacaoBruta

_ALIAS_NOME = ("nome", "razao_social", "razao social", "empresa")
_ALIAS_CIDADE = ("cidade", "municipio", "município")
_ALIAS_UF = ("uf", "estado", "sigla_uf")


def _decodificar(conteudo: bytes) -> str:
    try:
        return conteudo.decode("utf-8-sig")
    except UnicodeDecodeError:
        return conteudo.decode("latin-1")


def _detectar_delimitador(amostra: str) -> str:
    try:
        dialeto = csv.Sniffer().sniff(amostra, delimiters=",;")
        return dialeto.delimiter
    except csv.Error:
        return ","


def _mapear_colunas(cabecalhos: list[str]) -> dict[str, int]:
    normalizados = [c.strip().lower() for c in cabecalhos]
    mapa: dict[str, int] = {}
    for idx, coluna in enumerate(normalizados):
        if "nome" not in mapa and coluna in _ALIAS_NOME:
            mapa["nome"] = idx
        elif "cidade" not in mapa and coluna in _ALIAS_CIDADE:
            mapa["cidade"] = idx
        elif "uf" not in mapa and coluna in _ALIAS_UF:
            mapa["uf"] = idx
    return mapa


def ler_fornecedores_csv(conteudo: bytes) -> list[LinhaImportacaoBruta]:
    """Lê CSV com cabeçalho ``nome,cidade,uf`` (aliases aceitos)."""
    texto = _decodificar(conteudo).strip()
    if not texto:
        return []

    amostra = texto[:4096]
    delim = _detectar_delimitador(amostra)

    leitor = csv.reader(io.StringIO(texto), delimiter=delim)
    try:
        cabecalhos = next(leitor)
    except StopIteration:
        return []

    mapa = _mapear_colunas(cabecalhos)
    if not {"nome", "cidade", "uf"}.issubset(mapa):
        # Cabeçalho incompleto → lote vazio.
        return []

    resultado: list[LinhaImportacaoBruta] = []
    for numero_relativo, linha in enumerate(leitor, start=2):  # linha 1 = header
        if not linha or all(not celula.strip() for celula in linha):
            continue
        resultado.append(
            LinhaImportacaoBruta(
                nome=_celula(linha, mapa["nome"]),
                cidade=_celula(linha, mapa["cidade"]),
                uf=_celula(linha, mapa["uf"]),
                numero_linha=numero_relativo,
            )
        )
    return resultado


def _celula(linha: list[str], idx: int) -> str:
    return linha[idx] if idx < len(linha) else ""
