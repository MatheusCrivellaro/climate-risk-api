# ADR-015 — Iterador com filtro `municipios_alvo` para restaurar performance

**Status:** Aceito.
**Data:** 2026-04-28.

## Contexto

A Slice 21 (ADR-013) introduziu pipeline streaming via
`AgregadorEspacial.iterar_por_municipio` consumido em paralelo com `zip`
para as 3 variáveis (pr, tas, evspsbl). Performance em produção: ~30-50
min para 25 anos × 2 cenários × ~1700 municípios; memória estável em
~1.6 GB.

A Slice 22 (ADR-014) corrigiu um bug funcional: grades com coberturas
municipais distintas (especialmente bordas costeiras de modelos
climáticos) faziam o `zip` dos iteradores quebrar com
`RuntimeError: Inconsistência de iteração`. A solução foi calcular a
interseção dos 3 conjuntos `municipios_mapeados` e iterar por cada
município chamando `serie_de_municipio` 3 vezes (uma por grade).

A correção foi funcionalmente certa, mas degradou drasticamente a
performance. Em produção, com ~1557 municípios e 3 variáveis, a iteração
fez ~4671 chamadas a `serie_de_municipio`, cada uma disparando um
`compute` dask separado. Em medição:

- ~2h para processar ~64% do primeiro cenário (estimativa baseada em
  tamanho do db-wal)
- Estimativa total: ~6h por cenário, ~12h para os dois
- Equivalente da Slice 21 fazia o mesmo trabalho em ~30-50 min total

A causa raiz: `serie_de_municipio` é uma operação ponto-a-ponto
projetada para casos isolados (debug, exportação ad-hoc). Chamada em
loop por município, ela perde a localidade do streaming dask — cada
município reinicia uma cadeia de operações independente em vez de
continuar uma iteração sequencial sobre o mesmo `DataArray` lazy.

O usuário precisou parar o pipeline e descartar 2h de processamento.

## Decisão

**Refatorar `iterar_por_municipio` para aceitar parâmetro opcional
`municipios_alvo: set[int] | None = None`. Quando fornecido, o iterador
processa apenas `mapa.keys() & municipios_alvo` em ordem ascendente. O
handler passa a mesma interseção como filtro nas 3 chamadas e consome
com `zip(strict=True)`.**

Mudanças concretas:

1. **Porta `AgregadorEspacial.iterar_por_municipio`** ganha kwarg-only
   `municipios_alvo`. Sem o kwarg, comportamento idêntico à Slice 21
   (backward-compatible).
2. **Adapter `AgregadorMunicipiosGeopandas`** filtra `mapa["municipio_id"]`
   pelo conjunto-alvo antes do `sorted`, mantendo a ordem ascendente.
   O cache de mapeamento célula→município (em memória + parquet) é
   reaproveitado sem alteração.
3. **Handler `_processar_streaming`** volta ao padrão `zip(iter_pr,
   iter_tas, iter_evap)` da Slice 21, mas com `municipios_alvo=interseção`
   nas 3 chamadas. Como os 3 iteradores recebem o mesmo conjunto e
   percorrem em ordem ordenada, a sincronização é garantida por
   construção. Sanity checks (pareamento e tamanhos de série) ficam
   para detectar bugs futuros em implementações alternativas.
4. **`serie_de_municipio` é mantido** — continua útil para debug e
   análise pontual; docstring atualizada alertando contra uso em loop.
   Os testes da Slice 22 continuam válidos.

## Alternativas descartadas

### A — Materialização eager de séries em dict

Construir um `dict[int, ndarray]` com todas as séries das 3 variáveis
**antes** de iterar. Performance equivalente (1 compute dask por
variável), mas custa `~3 × N_municípios × N_dias × 8 bytes` na RAM —
para 1700 × 9000 dias = ~370 MB extra, dobrando a memória do pipeline.
Resolve o problema mas é menos elegante e desperdiça a oportunidade de
manter o streaming.

### B — Materialização em chunks de N municípios

Processar a interseção em chunks (ex.: 50 municípios por vez), cada
chunk materializando só suas séries. Reduz o custo de memória vs. A,
mas adiciona um eixo de tuning (`N_chunk`) e complexidade de pipeline
sem benefício claro vs. C. Over-engineering para o problema atual.

### C (escolhida) — Iterador com filtro `municipios_alvo`

- Memória O(1) adicional: nenhum buffer extra; cada município é cedido
  on-demand.
- Sincronização determinística garantida pela ordem ordenada do filtro
  — nenhum mecanismo extra de coordenação entre iteradores.
- Refatoração mínima: handler volta ao padrão `zip` da Slice 21, com
  apenas o kwarg novo.
- `serie_de_municipio` continua existindo para casos pontuais; só
  deixa de ser usado no caminho quente.

## Consequências

**Positivas:**

- Performance restaurada à da Slice 21 (~30-50 min para o cenário
  completo, vs. ~12h pós-Slice 22).
- Correção funcional da Slice 22 mantida — o handler ainda calcula a
  interseção antes de iterar e ainda emite warning estruturado das
  divergências.
- Memória continua O(1) por município (independente de `n_municipios`
  e `n_anos`).
- O kwarg `municipios_alvo` é genérico — pode ser reaproveitado para
  outras estratégias futuras (ex.: processar municípios prioritários
  primeiro, dividir por região, retomar de checkpoint).

**Negativas:**

- Adiciona um caminho de código (com vs. sem filtro) à
  `iterar_por_municipio`. Mitigação: o filtro é opcional e o caminho
  default é exatamente o da Slice 21, sem mudança de comportamento.
- O sanity check `mun_pr == mun_tas == mun_evap` no handler é
  redundante com a invariante "mesmo `municipios_alvo` + ordem
  ordenada → mesma sequência". Mantido como defesa em profundidade
  contra bugs em implementações alternativas do agregador.
- `serie_de_municipio` permanece como atrativa armadilha para callers
  desavisados que processem muitos municípios em loop. Mitigação: nota
  explícita no docstring referenciando esta ADR.

## Plano de revisão

Reavaliar quando:

- For necessário processar municípios em ordem não-ascendente (ex.:
  priorizar capitais, retomar de checkpoint específico). O filtro
  `municipios_alvo` cobre filtragem mas não ordenação custom; pode
  exigir um parâmetro novo (`ordem_municipios=...`).
- A interseção tornar-se um critério dinâmico (ex.: subconjunto por
  região do usuário). A primitiva atual já suporta — basta passar o
  recorte como `municipios_alvo`. Validar com caso real.
- For adicionada uma 4ª variável ao pipeline. A interseção de 4
  conjuntos continua trivial; o `zip` cresce para 4 iteradores e o
  sanity check para 4 IDs. Estrutura escala diretamente.
- Aparecer pressão para remover `serie_de_municipio`. Hoje preserva
  flexibilidade para análises ad-hoc com custo zero; remoção só
  faz sentido se nenhum caller real existir.
