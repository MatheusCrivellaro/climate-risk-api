# ADR-009 — Índices de Estresse Hídrico: Extensão Multi-Variável do Pipeline CORDEX

**Status:** Aceito.
**Data:** 2026-04-20.

## Contexto

O pipeline existente (ADR-001, Slices 1–4) calcula índices ETCCDI **sobre precipitação única** (`pr` em `mm/dia`): `wet_days`, `sdii`, `rx1day`, `rx5day`, `r20mm`, `r50mm`, `r95ptot`. Uma execução (`Execucao`) fica associada a um arquivo NetCDF único, lido pelo `LeitorXarray` e consumido pela calculadora em `domain/indices/calculadora.py`.

Surge um novo requisito funcional: calcular **frequência de dias secos quentes** e **intensidade do déficit hídrico** — ambos dependem de **três variáveis climáticas simultaneamente**:

| Variável | Grandeza | Unidade bruta CORDEX |
|---|---|---|
| `pr` | precipitação | `kg m-2 s-1` |
| `tas` | temperatura do ar a 2m | `K` |
| `evspsbl` | evaporação | `kg m-2 s-1` |

Inspeção dos arquivos reais disponíveis revela duas fontes com grades **incompatíveis entre si**:

| Fonte | Variáveis | Grade | Calendário |
|---|---|---|---|
| INPE-Eta SAM-20 | `pr`, `tas` | regular 1D `lat`×`lon` (390×355) | gregoriano padrão |
| SMHI-RCA4 SAM-44 | `evspsbl` | *rotated pole* 2D (167×146) | `noleap` |

Não há como co-registrar ponto-a-ponto sem reinterpolação — operação cara e que introduz erro numérico cuja validação externa não está no escopo do MVP.

## Decisão

**Estender o pipeline existente** com um novo `tipo` de execução, sem fragmentar o domínio:

- `Execucao.tipo` ganha um terceiro valor `"estresse_hidrico"` (hoje: `"grade_bbox"` para UC-02, `"pontos"` para UC-03). O valor antigo `"precipitacao_extrema"` — se houver rename futuro — fica **fora do escopo** desta decisão.
- Os índices novos coexistem com os antigos em `domain/indices/` mas ficam em módulo separado (`calculos/estresse_hidrico.py`), reaproveitando a entidade `ResultadoIndice` via novos valores de `nome_indice`:
  - `dias_secos_quentes_por_ano` (inteiro, contagem)
  - `intensidade_estresse_anual_mm` (float, soma do déficit nos dias secos quentes)
  - `deficit_hidrico_anual_mm` (float, soma anual do déficit, independente de dia seco)
- O leitor NetCDF ganha uma **nova porta paralela** (`LeitorMultiVariavel`), não estendemos o contrato atual (`LeitorNetCDF`) porque a assinatura `abrir(caminho, variavel)` é naturalmente uni-variável. A nova porta recebe três caminhos e retorna uma única entidade `DadosClimaticosMultiVariaveis`.
- **Agregação espacial por município acontece antes do cálculo** (próxima slice), resolvendo a incompatibilidade de grades: o município é o nível comum no output final (alinhado com o uso prático downstream — cobertura de fornecedores, consultas por UF/município) e a média ponderada por célula dentro do polígono é operação bem-definida em cada grade separadamente.

## Alternativas descartadas

### Reinterpolar as grades para um alvo comum

Rejeitada. `xarray.Dataset.interp` (bilinear) ou conservative regridding via `xesmf` resolveriam mecanicamente, mas introduzem erro numérico não-auditado — o time de dados ainda não validou qual método é aceitável para evaporação em grade *rotated pole*. Adiciona ~50 MB de dep (`xesmf` puxa `esmpy` via conda). Em vez disso, agregamos para município como passo de redução bem-definido em cada fonte.

### Entidade `ExecucaoClimada` separada

Rejeitada. Duplicaria repositório, status, casos de uso e schemas HTTP. Ambos os tipos de execução são CORDEX, ambos produzem `ResultadoIndice`, ambos são consumidos pela mesma lista paginada em `/execucoes`. Um novo valor de `tipo` expressa a variação com uma única coluna.

### Uma execução por arquivo fonte

Rejeitada. Fragmentaria o fluxo do usuário: cada índice de estresse hídrico precisa das três variáveis juntas — expor três execuções para uma única requisição lógica é vazar implementação para a interface.

## Consequências

**Positivas:**

- Schema `ResultadoIndice` reaproveitado sem migração (`municipio_id`, `nome_indice`, `valor`, `unidade` já existem).
- Novos índices coexistem sem afetar o pipeline de precipitação; o caminho existente continua bit-a-bit idêntico (garantido por `test_paridade_legacy.py`).
- Agregação por município antes do cálculo dá coerência entre `pr`/`tas`/`evap`: todos compartilham o mesmo eixo temporal e o mesmo índice espacial após a redução, eliminando a incompatibilidade de grade.
- Porta paralela (`LeitorMultiVariavel`) mantém o contrato uni-variável existente estável — zero risco de regressão no `LeitorXarray` atual.

**Negativas:**

- Perda de resolução espacial ponto-a-ponto para esses índices: o cálculo é **sempre** por município. Ficou consciente no product backlog — se surgir caso de uso com granularidade fina, será revisitado com regridding formal.
- Calendário `noleap` (`evspsbl`) precisa ser normalizado para gregoriano antes da interseção temporal. 29/02 em anos bissextos vira `NaN` no alinhamento — documentado no adapter; cálculos tratam `NaN` descartando o dia.
- A separação entre `calculos/` (novo) e `indices/` (existente) duplica uma categoria conceitual ("cálculos anuais de índices"). Aceitamos essa duplicação porque a fronteira é clara: `indices/` opera sobre uma série diária única, `calculos/` sobre três séries alinhadas.

## Plano de revisão

Reavaliar quando:

- Aparecer um terceiro tipo de execução multi-variável (sinaliza precisar generalizar a porta `LeitorMultiVariavel` para N variáveis).
- O time de dados decidir uma política oficial de regridding (permitiria retomar cálculos ponto-a-ponto sem agregação forçada).
- A carga de leitura de NetCDF crescer (hoje três arquivos por execução; cache em `xr.open_dataset` com `chunks={}` pode virar necessário).
