# ADR-013 — Pipeline streaming + persistência incremental para estresse hídrico

**Status:** Aceito (com correção — ver nota).
**Data:** 2026-04-27.

## Nota de correção (Slice 25 / ADR-016)

A premissa original deste ADR — "o pipeline streaming faz 3 computes
dask totais (um por variável)" — estava **incorreta**. O profiling
investigativo da Slice 24 (ver `scripts/bench_iterar_municipios.py` e
`tests/perf/profile_slice_23.txt`) revelou que a implementação fazia
`N_municipios × 3` computes, com overhead de scheduler dask
(~58 ms por chamada) dominando o tempo total — ~4,5 min só de overhead
em produção (1557 municípios × 3 variáveis).

A Slice 25 (ADR-016) corrigiu a implementação para **de fato** fazer
apenas 3 computes (um por variável), via `np.asarray(dados.values)`
único no início de `iterar_por_municipio` seguido de iteração com
NumPy puro. Bench sintético: ~38× mais rápido.

Os benefícios prometidos por este ADR continuam válidos: memória
controlada, retry idempotente via `deletar_por_execucao`, batches de
persistência (`BATCH_SIZE`), logs estruturados de progresso. O custo
de RAM passou de "~50 MB durante iteração" para "~540 MB total" —
ainda dentro do orçamento do worker.

## Contexto

A Slice 18 corrigiu a leitura de NetCDFs grandes (lazy via dask, ADR-010)
e a Slice 19 mudou a definição de intensidade. Nenhuma delas tocou no
agregador espacial nem no handler de estresse hídrico, que permaneciam
construindo um único `pd.DataFrame` global com todos os pontos × todos os
dias × 3 variáveis antes de calcular índices e persistir.

Com dados reais (25 anos × 1700 municípios × 3 variáveis), o caminho
quente da função `_agregar_por_municipio_com_mapa` estourou memória:

```
pyarrow.lib.ArrowMemoryError: realloc of size 134217728 failed
infrastructure/agregador_municipios_geopandas.py:200
  return pd.DataFrame(registros, columns=[...])
```

A causa raiz é arquitetural: tanto o agregador quanto o handler operam
em modo *eager* — coletam tudo em memória antes do próximo passo do
pipeline. A solução paliativa (chunks no DataFrame) só adiaria o
problema; o crescimento futuro (mais anos, mais municípios, mais
indicadores) o reintroduziria.

## Decisão

Tornar o pipeline streaming de ponta a ponta:

1. **Porta `AgregadorEspacial`** ganha método novo `iterar_por_municipio`
   que yield tuplas `(municipio_id: int, datas: ndarray, serie_diaria:
   ndarray)` sob demanda. Memória por iteração:
   `O(n_dias × n_celulas_do_municipio)` — algumas dezenas de KB no caso
   típico.
2. **Adapter `AgregadorMunicipiosGeopandas`** implementa o iterador
   ordenando municípios por ID (determinismo crítico para sincronizar
   três iterações paralelas pr/tas/evap) e usando vectorized indexing
   xarray para extrair só as células do município corrente — o restante
   do DataArray segue lazy via dask.
3. **Handler `_processar`/`_processar_de_pastas`** consome os 3
   iteradores em `zip`, calcula índices anuais por município e persiste
   em batches pequenos (`BATCH_SIZE = 100`). A pilha do handler nunca
   acumula mais que `BATCH_SIZE` entidades não persistidas.
4. **Idempotência**: o handler chama
   `RepositorioResultadoEstresseHidrico.deletar_por_execucao` no início
   para apagar parciais de tentativas anteriores antes de recomeçar. Sem
   isso, retries esbarrariam na `UniqueConstraint(execucao_id,
   municipio_id, ano)`.

O método legacy `agregar_por_municipio` (formato DataFrame eager) é
preservado para os testes existentes do agregador e para callers
externos que ainda dependam dele. Documenta-se que não deve ser usado em
datasets grandes.

## Alternativas descartadas

### B — Chunks de DataFrame mantendo o pipeline eager

Quebrar `_agregar_por_municipio_com_mapa` em chunks de N municípios e
persistir entre chunks. Funciona, mas mantém o acoplamento "agregar tudo
→ processar tudo" e a memória por chunk ainda é proporcional a
`n_municipios_chunk × n_dias`. Crescer um eixo (mais anos, indicadores
adicionais) reintroduziria o estouro.

### C — Otimização micro com NumPy mantendo o DataFrame final

Reescrever `_agregar_por_municipio_com_mapa` com arrays NumPy puros e
evitar `pd.DataFrame(records, ...)` final, devolvendo um array
estruturado. Reduz o consumo, mas não resolve o estouro: o pico ocorre
porque o pipeline materializa o cross-product `municipios × dias × 3
variáveis` antes do cálculo. A redução de constante não muda a ordem
de grandeza.

### A (escolhida) — Streaming + persistência incremental

Mais ampla que B/C, com benefícios extra:

- Memória independente de `n_municipios` e `n_anos`. Crescer qualquer eixo
  só aumenta o tempo, não a RAM.
- Logging estruturado de progresso fica natural (já estamos iterando,
  basta logar a cada N municípios).
- Idempotência via `deletar_por_execucao` torna retries do worker
  seguros em qualquer ponto do pipeline.

## Consequências

**Positivas:**

- Pipeline aguenta o cenário real (25 anos × 1700 municípios) e
  cresce graciosamente para qualquer combinação razoável.
- Retries do worker são idempotentes — falha no meio do pipeline pode
  ser refeita sem cleanup manual.
- Logs estruturados (`execucao_id`, `municipios_processados`,
  `resultados_persistidos`) facilitam observabilidade quando algo
  trava em produção.

**Negativas:**

- Refatoração ampla. Touched files: porta `AgregadorEspacial`, adapter
  `AgregadorMunicipiosGeopandas`, porta
  `RepositorioResultadoEstresseHidrico` (e impl SQLAlchemy), ambos os
  handlers de estresse hídrico, todos os testes correspondentes.
- A coordenação entre 3 iteradores precisa de cuidado: ordem
  determinística no agregador é precondição. Se algum dia surgir uma
  reimplementação do agregador, ela precisa preservar ordem ou o
  handler levanta erro descritivo (`"Inconsistência de iteração"`).
- O método legacy `agregar_por_municipio` continua presente — vive
  como camada fina para compatibilidade, mas pode confundir leitores
  novos. Documentado tanto na porta quanto no adapter como "não usar em
  dataset grande".

## Plano de revisão

Reavaliar quando:

- Aparecer pressão para mais um indicador no formato wide. A coordenação
  agregador→handler suporta sem mudar o protocolo, mas vale revisar se a
  estrutura `_zip_iteradores` continua adequada com 4+ variáveis.
- For necessário processar municípios em paralelo (multiprocessing ou
  asyncio gather). Hoje processamos sequencialmente por simplicidade —
  a ordem determinística vira ainda mais importante.
- O método legacy `agregar_por_municipio` deixar de ter callers reais.
  Nesse momento podemos remover sem quebrar contratos.
