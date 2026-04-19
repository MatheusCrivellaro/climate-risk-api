"""Parser do formato legacy ``CIDADE/UF`` por linha.

Substitui a célula ``pd.read_excel("Localizacao_fornecedores.xlsx")`` do
notebook ``locais_faltantes_fornecedores.ipynb`` — o usuário cola o
conteúdo bruto no campo ``texto_legacy`` do endpoint
``POST /cobertura/fornecedores`` e recebemos uma lista estruturada.
"""

from __future__ import annotations

from climate_risk.application.cobertura.cobertura_fornecedores import FornecedorEntrada


def parsear_lista_legacy(texto: str) -> list[FornecedorEntrada]:
    """Converte texto livre do formato legado em :class:`FornecedorEntrada`.

    Cada linha não-vazia no formato ``CIDADE/UF`` vira um fornecedor. O
    ``identificador`` ecoa a linha original (após *strip*) para permitir
    correlação na resposta. Linhas vazias, só espaços ou sem ``/`` são
    silenciosamente ignoradas (o arquivo real tem rodapés e comentários).

    Args:
        texto: Conteúdo bruto (``.splitlines()`` interno).

    Returns:
        Lista de :class:`FornecedorEntrada` na ordem de aparição.
    """
    resultados: list[FornecedorEntrada] = []
    for linha_bruta in texto.splitlines():
        linha = linha_bruta.strip()
        if not linha or "/" not in linha:
            continue
        cidade_bruta, uf_bruta = linha.rsplit("/", 1)
        cidade = cidade_bruta.strip()
        uf = uf_bruta.strip().upper()
        if not cidade or not uf:
            continue
        resultados.append(
            FornecedorEntrada(
                identificador=linha,
                cidade=cidade,
                uf=uf,
            )
        )
    return resultados
