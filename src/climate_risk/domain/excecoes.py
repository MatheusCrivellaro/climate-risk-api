"""Exceções do domínio.

Hierarquia de exceções de negócio levantadas pelas entidades e repositórios.
Cada caso de uso/slice pode adicionar subclasses específicas conforme
necessário (ex.: ``ErroArquivoNCNaoEncontrado`` virá no Slice 3).
"""

from __future__ import annotations


class ErroDominio(Exception):
    """Base de todas as exceções de domínio.

    Capturar esta classe permite separar erros de negócio de erros técnicos
    inesperados na camada de middleware (ver ``interfaces/middleware/erros.py``).
    """


class ErroEntidadeNaoEncontrada(ErroDominio):
    """Levantada quando um repositório não encontra a entidade por ID.

    Args:
        entidade: Nome da entidade procurada (ex.: ``"Municipio"``).
        identificador: Chave primária usada na busca.
    """

    def __init__(self, entidade: str, identificador: str) -> None:
        self.entidade = entidade
        self.identificador = identificador
        super().__init__(f"{entidade} '{identificador}' não encontrado(a).")


class ErroConflito(ErroDominio):
    """Violação de unicidade ou integridade lógica.

    Exemplos: tentar importar o mesmo fornecedor com o mesmo
    ``identificador_externo`` duas vezes; salvar uma entidade cujo ID já existe
    em um contexto que exige ``INSERT`` (e não ``UPSERT``).
    """
