"""Exceções do domínio.

Hierarquia de exceções de negócio levantadas pelas entidades, repositórios
e adaptadores. O middleware HTTP (``interfaces/middleware/erros.py``) pode
capturar ``ErroDominio`` como raiz para traduzir para RFC 7807.
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


class ErroLeituraNetCDF(ErroDominio):
    """Base de todos os erros ao ler um arquivo NetCDF.

    Levantada pelo adaptador de leitura (``infrastructure/netcdf/``) e
    também suas subclasses — esta classe garante que nenhuma exceção crua
    de ``xarray``/``netCDF4``/``h5netcdf`` vaze para camadas superiores.

    Args:
        caminho: Path do arquivo ``.nc`` que estava sendo lido.
        detalhe: Mensagem descrevendo a causa (opcionalmente
            ``str(erro_original)`` quando há encadeamento).
    """

    def __init__(self, caminho: str, detalhe: str) -> None:
        self.caminho = caminho
        self.detalhe = detalhe
        super().__init__(f"Falha ao ler NetCDF '{caminho}': {detalhe}")


class ErroArquivoNCNaoEncontrado(ErroLeituraNetCDF):
    """Arquivo ``.nc`` não existe ou não está acessível no filesystem.

    Levantada antes de qualquer tentativa de abrir o dataset, evitando
    expor tracebacks obscuros do backend de NetCDF.
    """


class ErroVariavelAusente(ErroLeituraNetCDF):
    """A variável climática esperada não está presente no dataset.

    Args:
        caminho: Path do arquivo ``.nc``.
        variavel: Nome da variável procurada (ex.: ``"pr"``).
    """

    def __init__(self, caminho: str, variavel: str) -> None:
        self.variavel = variavel
        super().__init__(
            caminho=caminho,
            detalhe=f"variável '{variavel}' ausente no dataset.",
        )


class ErroDimensaoTempoAusente(ErroLeituraNetCDF):
    """A variável existe mas não possui a dimensão ``time``.

    Args:
        caminho: Path do arquivo ``.nc``.
        variavel: Nome da variável sem dimensão temporal.
    """

    def __init__(self, caminho: str, variavel: str) -> None:
        self.variavel = variavel
        super().__init__(
            caminho=caminho,
            detalhe=f"variável '{variavel}' não possui dimensão 'time'.",
        )


class ErroCoordenadasLatLonAusentes(ErroLeituraNetCDF):
    """Não foi possível identificar coordenadas de latitude/longitude.

    Levantada quando nenhum dos nomes reconhecidos (``lat``/``latitude``/``y``
    combinado com ``lon``/``longitude``/``x``) aparece em ``coords`` ou
    ``variables`` do dataset.
    """


class ErroLimitePontosSincrono(ErroDominio):
    """Requisição síncrona por pontos excede o limite configurado.

    UC-03 aceita até ``settings.sincrono_pontos_max`` pontos no caminho
    síncrono (``POST /calculos/pontos``). Acima desse limite, a API deve
    rejeitar com 400 — e, a partir do Slice 7, devolver 202 enfileirando
    um job.

    Args:
        total: Quantidade de pontos recebidos na requisição.
        maximo: Limite síncrono permitido (valor de
            ``settings.sincrono_pontos_max``).
    """

    def __init__(self, total: int, maximo: int) -> None:
        self.total = total
        self.maximo = maximo
        super().__init__(
            f"Total de pontos ({total}) excede o limite síncrono ({maximo}). "
            "Use processamento assíncrono para volumes maiores."
        )
