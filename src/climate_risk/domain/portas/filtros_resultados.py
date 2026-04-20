"""DTOs de filtro da porta :class:`RepositorioResultados` (Slice 11).

Estes dataclasses são usados pelos métodos ``consultar``, ``contar_por_filtros``
e ``agregar`` da porta. Ficam em ``domain/`` porque a porta é Protocol de
domínio — a camada ``application`` constrói instâncias destes tipos e os
passa adiante. Nenhum campo carrega dependências de ``raio_km``/Haversine:
o caso de uso converte raio em BBOX antes de delegar ao repositório.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FiltrosConsultaResultados:
    """Condições AND aplicadas às consultas de resultado.

    Campos ``None`` / sequências vazias não entram no WHERE.

    Attributes:
        execucao_id: Filtra por uma execução específica.
        cenario: Exige JOIN com ``execucao`` para filtrar pelo cenário
            declarado na execução (ex.: ``"rcp45"``).
        variavel: Análogo para ``execucao.variavel`` (ex.: ``"pr"``).
        ano: Ano exato (``=``).
        ano_min: Limite inferior inclusivo de ano.
        ano_max: Limite superior inclusivo de ano.
        nomes_indices: Conjunto fechado de nomes de índice; quando vazio,
            todos entram. Campo plural para permitir IN(...) (vários valores
            separados por vírgula na querystring).
        lat_min/lat_max/lon_min/lon_max: BBOX espacial; ``lon_min > lon_max``
            indica cruzamento do antimeridiano (OR de duas faixas em
            longitude).
        uf: Filtra via JOIN com ``municipio``; exige ``municipio_id`` não
            nulo no resultado.
        municipio_id: Filtro direto por município enriquecido.
    """

    execucao_id: str | None = None
    cenario: str | None = None
    variavel: str | None = None
    ano: int | None = None
    ano_min: int | None = None
    ano_max: int | None = None
    nomes_indices: tuple[str, ...] = ()
    lat_min: float | None = None
    lat_max: float | None = None
    lon_min: float | None = None
    lon_max: float | None = None
    uf: str | None = None
    municipio_id: int | None = None


@dataclass(frozen=True)
class FiltrosAgregacaoResultados:
    """Agrega a consulta por uma ou mais dimensões.

    Attributes:
        filtros: WHERE aplicado antes do ``GROUP BY``.
        agregacao: Função aplicada a ``valor`` — ``"media"``, ``"min"``,
            ``"max"``, ``"count"``, ``"p50"``, ``"p95"``.
        agrupar_por: Dimensões do ``GROUP BY``. Valores suportados:
            ``"ano"``, ``"cenario"``, ``"variavel"``, ``"nome_indice"``,
            ``"municipio"``. A ordem define a ordem das colunas no dict
            ``grupo``.
    """

    filtros: FiltrosConsultaResultados = field(default_factory=FiltrosConsultaResultados)
    agregacao: str = "media"
    agrupar_por: tuple[str, ...] = ()


@dataclass(frozen=True)
class GrupoAgregadoRaw:
    """Uma linha do resultado de ``agregar``.

    Attributes:
        grupo: Chaves do ``GROUP BY`` mapeadas para os valores daquele grupo
            (ex.: ``{"ano": 2026, "cenario": "rcp45"}``). Para agregações
            globais (sem ``agrupar_por``), fica ``{}``.
        valor: Resultado da função de agregação. ``None`` quando a função
            é indefinida para o grupo (ex.: média de zero amostras com
            ``valor`` não nulo).
        n_amostras: Quantidade de linhas que compuseram o grupo (inclui
            amostras com ``valor IS NULL`` para ``count``; exclui essas
            para as outras agregações).
    """

    grupo: dict[str, str | int]
    valor: float | None
    n_amostras: int
