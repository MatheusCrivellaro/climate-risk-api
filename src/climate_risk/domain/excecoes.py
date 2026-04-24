"""Exceções do domínio.

Hierarquia de exceções de negócio levantadas pelas entidades, repositórios
e adaptadores. O middleware HTTP (``interfaces/middleware/erros.py``) pode
capturar ``ErroDominio`` como raiz para traduzir para RFC 7807.
"""

from __future__ import annotations

from pathlib import Path


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


class ErroJobNaoEncontrado(ErroDominio):
    """Tentativa de operar em um :class:`Job` inexistente.

    Levantada por casos de uso (consulta, reprocessamento) ao traduzir um
    ``job_id`` recebido externamente. Na camada HTTP o middleware converte
    para ``404``.

    Args:
        job_id: Identificador do job procurado.
    """

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job '{job_id}' não encontrado.")


class ErroClienteIBGE(ErroDominio):
    """Falha ao comunicar com a API de Localidades/Malhas do IBGE.

    Encapsula qualquer erro de rede ou HTTP não-2xx persistente após as
    retentativas configuradas. O middleware HTTP traduz para ``503 Service
    Unavailable`` — é um erro transitório do ponto de vista do cliente.

    Args:
        mensagem: Descrição curta da falha (ex.: ``"timeout"``).
        endpoint: URL ou caminho relativo que estava sendo consultado.
    """

    def __init__(self, mensagem: str, endpoint: str) -> None:
        self.mensagem = mensagem
        self.endpoint = endpoint
        super().__init__(f"Falha ao consultar IBGE ({endpoint}): {mensagem}")


class ErroConfiguracao(ErroDominio):
    """Configuração ausente ou inválida impede a operação.

    Levantada quando um adaptador depende de um caminho, credencial ou
    variável de ambiente que o operador não preencheu — por exemplo, o
    shapefile de municípios (``shapefile_mun_path``) referenciado pelos
    endpoints ``/localizacoes/localizar`` e ``/cobertura/fornecedores``
    no Slice 9. O middleware HTTP mapeia para ``500`` com mensagem clara
    (é erro do operador, não do cliente).

    Args:
        mensagem: Descrição do problema (ex.: ``"shapefile_mun_path
            não configurado"``).
    """

    def __init__(self, mensagem: str) -> None:
        super().__init__(mensagem)


class ErroFormatoInvalido(ErroDominio):
    """Arquivo em formato não suportado (esperado CSV ou XLSX).

    Levantada pela rota ``POST /fornecedores/importar`` quando a extensão
    do arquivo recebido não é reconhecida. O middleware HTTP mapeia para
    ``400 Bad Request`` — é erro do cliente.

    Args:
        mensagem: Descrição curta do problema (ex.:
            ``"formato '.txt' não suportado — use .csv ou .xlsx"``).
    """

    def __init__(self, mensagem: str) -> None:
        super().__init__(mensagem)


class ErroValidacao(ErroDominio):
    """Combinação inválida ou incoerente de parâmetros de entrada.

    Diferente das validações de tipo/forma (resolvidas pelo Pydantic com
    ``422``), esta exceção expressa regras de negócio envolvendo
    **combinações** de campos — ex.: ``raio_km`` fornecido sem
    ``centro_lat``/``centro_lon``, ``agrupar_por`` com chave
    desconhecida. O middleware HTTP mapeia para ``422 Unprocessable
    Entity``.

    Args:
        mensagem: Descrição curta e orientada ao cliente.
    """

    def __init__(self, mensagem: str) -> None:
        super().__init__(mensagem)



class ErroGradeDesconhecida(ErroDominio):
    """DataArray sem coordenadas ``lat``/``lon`` identificáveis.

    Levantada pelo :class:`AgregadorEspacial` quando o ``DataArray`` não
    expõe ``lat`` e ``lon`` nem como arrays 1D (grade regular) nem como
    arrays 2D pré-calculados (grade rotacionada com coordenadas
    auxiliares). Cobre casos anômalos em que o adaptador não tem como
    mapear células → (lat, lon) sem uma desrotação customizada.

    Args:
        mensagem: Descrição com as coordenadas efetivamente disponíveis
            no ``DataArray``, para facilitar o diagnóstico.
    """

    def __init__(self, mensagem: str) -> None:
        super().__init__(mensagem)

class ErroShapefileMunicipiosIndisponivel(ErroDominio):
    """Shapefile de municípios configurado mas inacessível.

    Levantada pelo adaptador de agregação espacial ao ser instanciado
    quando o caminho configurado em ``settings.shapefile_mun_path`` não
    existe ou não é legível. Diferente de :class:`ErroConfiguracao`, que
    cobre o caso de configuração **ausente**, esta exceção sinaliza uma
    configuração **presente mas inválida** — é defeito de operação, não
    defeito de ambiente.

    Args:
        mensagem: Descrição do problema, incluindo o caminho tentado.
    """

    def __init__(self, mensagem: str) -> None:
        super().__init__(mensagem)
        
class ErroCenarioInconsistente(ErroDominio):
    """Arquivos de um mesmo lote multi-variável reportam cenários diferentes.

    Levantada pelo :class:`LeitorCordexMultiVariavel` (Slice 13) quando os
    três arquivos (``pr``/``tas``/``evspsbl``) passados numa mesma execução
    de estresse hídrico não concordam no atributo ``experiment_id`` (nem
    no regex extraído do nome do arquivo). O middleware HTTP traduz para
    ``422`` — é erro de uso: o operador forneceu arquivos incompatíveis.

    Args:
        cenarios: Mapa ``{caminho: cenario}`` com o valor detectado por
            arquivo, preservando a ordem de entrada para diagnóstico.
    """

    def __init__(self, cenarios: dict[str, str]) -> None:
        self.cenarios = dict(cenarios)
        detalhes = ", ".join(f"{Path(k).name}={v}" for k, v in cenarios.items())
        super().__init__(
            f"Arquivos reportam cenários divergentes: {detalhes}. "
            "Todos os arquivos precisam compartilhar o mesmo cenário."
        )


class ErroJobEstadoInvalido(ErroDominio):
    """Transição de estado não permitida para um :class:`Job`.

    Exemplos: reprocessar um job que ainda está ``pending`` (apenas
    ``failed`` é elegível); cancelar um job que já está ``completed``.

    Args:
        job_id: Identificador do job.
        estado_atual: Status atual, um dos valores de :class:`StatusJob`.
        transicao: Nome legível da transição solicitada (ex.: ``"retry"``,
            ``"cancelar"``).
    """

    def __init__(self, job_id: str, estado_atual: str, transicao: str) -> None:
        self.job_id = job_id
        self.estado_atual = estado_atual
        self.transicao = transicao
        super().__init__(f"Job '{job_id}' em estado '{estado_atual}' não permite '{transicao}'.")
