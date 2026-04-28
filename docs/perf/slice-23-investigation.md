# Investigação de Performance — Slice 23 vs Slice 21

**Slice:** 24 (investigativa).
**Data:** 2026-04-28.
**Status:** Concluída — gargalo identificado, recomendação para Slice 25.

## Contexto

O pipeline de estresse hídrico passou por 3 iterações:

- **Slice 21** (ADR-013) — Pipeline streaming via
  `iterar_por_municipio` consumido em paralelo com `zip` para pr/tas/evap.
  Performance reportada em produção: ~30-50 min para 25 anos × ~1700
  municípios × 2 cenários. Bug funcional: travava com grades divergentes
  (`RuntimeError: Inconsistência de iteração`).
- **Slice 22** (ADR-014) — Substituiu o `zip` por loop por município com
  3 chamadas a `serie_de_municipio`. Funcionalmente correta (interseção
  + warning estruturado), mas degradou drasticamente em produção
  (~12h estimadas para os 2 cenários).
- **Slice 23** (ADR-015) — Refatorou `iterar_por_municipio` para aceitar
  `municipios_alvo`. Voltou ao padrão `zip` da Slice 21 mas com filtro.
  Performance ainda inviável em produção (~10h estimadas).

A premissa em ADR-015 era: "restaura performance da Slice 21 (~3
computes dask em vez de O(N_municípios × 3))". Esta investigação testou
essa premissa.

## Hipóteses iniciais

1. O `municipios_alvo` (set filtering) quebra alguma otimização de
   leitura sequencial dask.
2. O `zip(strict=True)` força sincronização que não existia na Slice 21.
3. O `set(mapa.keys()) & municipios_alvo` toda vez é caro.
4. Diferença de versão dask/xarray entre Slice 21 e Slice 23.
5. Padrão de chunks dask mudou entre as Slices.

A revisão do código-fonte (commit `59b01a0` da Slice 21 vs HEAD da
Slice 23) já permitia descartar (1)–(5) por inspeção: o caminho quente
do iterador é **idêntico** nas duas implementações; o filtro
`municipios_alvo` apenas restringe o conjunto antes do `sorted()` —
tudo o mais (`isel` vetorizado, `.mean(dim="cell").values`) é o mesmo.
A hipótese a confirmar empiricamente passou a ser:

6. **Ambas Slice 21 e Slice 23 têm o mesmo gargalo** — N×3 `compute`
   dask por chamada — e a memória de "30-50 min" da Slice 21 é
   inconsistente com o que o código realmente faz, ou foi medida em
   condições diferentes (dataset menor, IO mais rápido, etc.).

## Setup do benchmark

`scripts/bench_iterar_municipios.py` constrói 3 DataArrays
``(time, y, x)`` dask-backed com:

- ``ny × nx ≈ n_municipios × 16`` células (média ~16 células/município).
- ``n_dias = n_anos × 365`` timestamps.
- Chunks dask: ``(time=365, y=ny/4, x=nx/4)`` — replica o padrão do leitor
  CORDEX (chunks=time:365 conforme ADR-010).
- ``float32`` aleatórios determinísticos (seed por nome de variável).

Mapeamento célula→município:

- ``mapa_pr`` e ``mapa_tas`` compartilham o mesmo conjunto (mesmo modelo).
- ``mapa_evap`` realoca ~20% das células para municípios novos —
  reproduz a divergência de cobertura observada em produção (ADR-014).

Quatro estratégias rodam contra essas mesmas fixtures:

| Estratégia       | Padrão                                                                |
|------------------|-----------------------------------------------------------------------|
| `slice_21`       | `zip(iter_pr, iter_tas, iter_evap)` sem filtro (mapas pré-restritos)  |
| `slice_22`       | `for municipio in interseção: serie_de_municipio × 3`                 |
| `slice_23`       | `zip(iter_pr, iter_tas, iter_evap)` com `municipios_alvo=interseção`  |
| `materializado`  | `dados.values` 1× por variável + indexação numpy (proposta Slice 25)  |

Cada estratégia é medida 2 vezes: tempo wallclock limpo + cProfile
(top 30 cumulativo + top 30 self-time). Profiles salvos em
`tests/perf/profile_<nome>.txt`.

**Hardware do bench:** Linux x86_64, Python 3.12.3, 4 CPUs.
**Versões:** xarray 2026.4.0, dask 2026.3.0, numpy 2.4.4, pandas 3.0.2.

## Resultados

### Tempos wallclock

#### 100 municípios × 5 anos (default)

| Estratégia       | Wallclock | Municípios/seg |
|------------------|----------:|---------------:|
| slice_21         |  10.58 s  |  9.5           |
| slice_22         |  10.03 s  |  10.0          |
| slice_23         |  10.73 s  |  9.3           |
| **materializado**|   0.26 s  | **380.3**      |

#### 300 municípios × 10 anos

| Estratégia       | Wallclock | Municípios/seg |
|------------------|----------:|---------------:|
| slice_21         |  76.48 s  |  3.9           |
| slice_22         |  77.04 s  |  3.9           |
| slice_23         |  79.04 s  |  3.8           |
| **materializado**|   1.98 s  | **151.5**      |

Slice 21, 22 e 23 ficam **dentro de ~3% entre si** — a diferença é
ruído de scheduler de threads, não estrutural. A estratégia
`materializado` é **~38× mais rápida** consistentemente, e escala bem
melhor (queda de 380 → 152 mun/seg quando 10× mais dados, contra 9.5 →
3.9 das streaming).

### Análise dos profiles

#### Slice 21 e Slice 23 (idênticos no padrão de chamadas)

Top funções por tempo cumulativo (`profile_slice_21.txt`,
`profile_slice_23.txt`):

```
ncalls  tottime  cumtime  função
  300    0.020   17.525   dask/base.py:353(compute)
  303    0.000   17.541   xarray/.../variable.py:321(_as_array_or_item)
  300    0.001   17.537   dask/array/core.py:1706(__array__)
31820    0.060   25.792   dask/local.py:140(queue_get)
```

- **300 chamadas a `dask.base.compute`** — exatamente 100 municípios ×
  3 variáveis. Cada `compute` custa ~58 ms (dominado por orquestração
  de threads, não cálculo numérico).
- 31820 chamadas a `dask/local.py:queue_get` — o scheduler de threads
  do dask é invocado massivamente: ~106 chamadas de `queue.get` por
  `compute()`.
- Top self-time: `dask/array/core.py:6030(_vindex_slice_and_transpose)`
  e `dask/order.py:618(_connecting_to_roots)` — overhead estrutural do
  dask preparando o grafo a cada `compute`.

#### Slice 22

Top funções por tempo cumulativo (`profile_slice_22.txt`): mesmo padrão
do Slice 21/23. **300 computes**, mesma orquestração do dask. Diferença
estrutural com 21/23 é apenas a forma do loop em Python (`zip` vs
`for municipio: serie_de_municipio × 3`); o caminho quente é o mesmo.

#### Materializado

Top funções por tempo cumulativo (`profile_materializado.txt`):

```
ncalls  tottime  cumtime  função
    3    0.000    0.219   dask/base.py:353(compute)
    3    0.000    0.232   xarray/.../variable.py:321(_as_array_or_item)
  483    0.001    0.406   dask/local.py:140(queue_get)
```

- **3 chamadas a `compute`**, exatamente 1 por variável. Cada uma custa
  ~73 ms (lê o array todo, não 16 células).
- 483 chamadas a `queue_get` — escala com chunks lidos, não com
  municípios.
- O resto do tempo (~0.19s de 0.42s) é `pandas.groupby` + `numpy.nanmean`,
  muito barato.

### Gargalo identificado

A premissa em ADR-015 — "Slice 21 fazia 1 compute por variável e a
Slice 22 quebrou isso" — está **incorreta**. As duas Slices fazem
``N_municípios × 3`` `compute` dask. O custo dominante de cada `compute`
é a orquestração do scheduler de threads (preparação de grafo,
`queue.get`, threading), não a leitura dos dados em si — cada município
mexe com ~16 células × 1825 dias = ~30 KB, mas paga ~58 ms de overhead
fixo por compute.

A degradação do Slice 21 → 22 → 23 em produção provavelmente reflete
**outras** mudanças paralelas (versão de dask/xarray, padrões de chunk
do leitor, IO de NetCDFs reais) que amplificam o overhead já presente
desde a Slice 21. O código nunca foi rápido por design — apenas pareceu
rápido o suficiente em medição inicial.

A diferença de ~3% entre `slice_21` (sem filtro) e `slice_23` (com
filtro) no benchmark mostra que `municipios_alvo` **não** introduz
overhead relevante. Hipóteses (1)–(3) descartadas.

## Conclusão e recomendação

**Hipótese confirmada:** o gargalo é arquitetural — todos os 3 padrões
disparam ``O(n_municípios × 3)`` `compute` dask. Cada `compute` carrega
~58 ms de overhead fixo de scheduler que **domina** sobre o cálculo
numérico. A diferença entre Slice 21 e Slice 23 está dentro do ruído de
threading.

**Recomendação para a Slice 25 — materializar uma vez por variável:**

1. Refatorar `AgregadorMunicipiosGeopandas.iterar_por_municipio` para
   chamar `dados.values` (ou `dados.compute()`) **uma única vez por
   variável** logo no início, antes do loop de municípios. O custo de
   memória é ``n_dias × ny × nx × 4 bytes`` (float32):
   - 25 anos × 50×50 grade ≈ 91 MB / variável; 273 MB total para 3
     variáveis. Aguenta com folga.
   - Mesmo a 25 anos × 200×200 grade ≈ 1.4 GB / variável — ainda
     viável; se virar problema, processa em janelas temporais.
2. Substituir o `dados.isel(...).mean(dim="cell").values` por
   indexação numpy direta no array materializado, replicando o que
   `_agregar_por_municipio_com_mapa` (legacy) já faz. Os profiles
   mostram que esse caminho custa <1 ms/município.
3. Adicionalmente: pré-computar o agrupamento `mapa.groupby("municipio_id")`
   uma vez antes do loop, em vez do `mapa[mapa["municipio_id"] == mid]`
   atual (custo O(N×K) hoje, baixo no benchmark mas escala mal com
   ~1700 municípios e ~5000 células reais).

**Ordem de grandeza esperada na Slice 25**, extrapolando o benchmark:

- Hoje (Slice 23) em produção: ~10 h por par de cenários.
- Pós Slice 25, ~38× mais rápido na orquestração: **~15-20 min** por
  par de cenários, alinhado com o que ADR-013 originalmente prometeu.

**Riscos / pontos a validar antes de implementar:**

- Materializar pode consumir mais RAM do que o pipeline streaming
  prometia evitar. Documentar e medir RAM real (e.g. via `tracemalloc`)
  no teste de integração existente
  (`tests/integration/test_pipeline_streaming.py` tem o limite de
  200 MB — pode precisar relaxar para ~1 GB ou processar em janelas
  temporais).
- O leitor `LeitorMultiVariavel` abre arquivos com `xr.open_mfdataset`
  e chunks lazy. `dados.values` força leitura completa, perdendo o
  beneficio de streaming I/O. Para datasets enormes (não é o caso
  típico), recomendar implementação alternativa baseada em chunks
  temporais.

**O que NÃO é a Slice 25:**

- Não é "voltar para Slice 21" — Slice 21 já tinha o mesmo gargalo.
- Não é "reparalelizar com asyncio" — overhead vem do scheduler dask,
  não de I/O síncrono.
- Não é "remover o `municipios_alvo`" — o filtro não é o problema.

**Validação adicional sugerida (Slice 25 ou paralela):** rodar
`scripts/bench_iterar_municipios.py` com dados reais (CORDEX + IBGE) em
vez de fixtures sintéticas, para confirmar que o ganho de ~38× se
mantém com NetCDFs reais e cache de mapeamento de produção.
