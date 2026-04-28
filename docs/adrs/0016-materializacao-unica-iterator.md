# ADR-016 — Materialização única no iterator de agregação

**Status:** Aceito.
**Data:** 2026-04-28.

## Contexto

A Slice 21 (ADR-013) introduziu o pipeline streaming via
`AgregadorEspacial.iterar_por_municipio`, com a premissa de que cada
chamada ao iterator faria **um único** `compute` dask por variável (3
no total). A Slice 22 (ADR-014) e a Slice 23 (ADR-015) refinaram o
contrato (interseção de cobertura, filtro `municipios_alvo`) sem
revisitar essa premissa.

A Slice 24 (profiling investigativo) mostrou que a premissa estava
**incorreta**. A implementação anterior usava
`xr.DataArray.isel(...).mean(dim="cell").values` dentro do loop, e cada
`.values` aciona o scheduler dask para a sub-grade do município.
Mensurou-se ~58 ms de overhead de scheduler por município por variável.
Em produção (1557 municípios × 3 variáveis):

```
1557 × 3 × 58 ms ≈ 271 s = ~4,5 min só de overhead
```

Bench sintético (300 municípios × 10 anos):

| Implementação                          | Tempo    |
| -------------------------------------- | -------- |
| Slice 21 (eager DataFrame)             | 76,5 s   |
| Slice 22 (interseção, sem filtro)      | 77,0 s   |
| Slice 23 (interseção + filtro)         | 79,0 s   |
| Materializado (`dados.values` único)   |  1,98 s  |

A diferença entre 21–23 ficou dentro de ~3 % (ruído estatístico). A
materialização única é ~38× mais rápida.

Custo de RAM estimado (25 anos × 9131 dias × ~5000 células × float32):

- Por variável: ~180 MB
- Total das 3 variáveis: ~540 MB

O worker hoje opera em ~1,6 GB sem suar. Cabe folgado.

## Decisão

`AgregadorMunicipiosGeopandas.iterar_por_municipio` chama
`np.asarray(dados.values)` **uma única vez** no início, virando um array
NumPy 2D `(n_tempo, n_celulas)`. Em seguida, itera por município com
slicing NumPy puro + `np.nanmean(..., axis=1)` — sem dask no loop.

Otimização extra: quando `municipios_alvo` resulta em conjunto vazio, o
método retorna sem materializar o array (zero compute dask).

A interface pública não muda: continua produzindo
`Iterator[tuple[int, np.ndarray, np.ndarray]]` em ordem ascendente
determinística. O handler (`_processar_streaming`) e os testes das
Slices 21–23 permanecem inalterados.

## Alternativas descartadas

- **Manter streaming dask**: ~38× mais lento, comprovado por bench.
- **Reescrever o iterator com chunks dask explícitos**: complexidade
  alta, ganho marginal. O gargalo era scheduler overhead, não cálculo.
- **Cachear o resultado em disco**: não resolve — o compute ainda
  rodaria toda execução nova.

## Consequências

### Positivas

- Performance ~38× melhor (bench sintético).
- Tempo total estimado para 2 cenários × 25 anos: ~15-25 min (vs ~10 h
  em produção antes).
- Compatível com `municipios_alvo` da Slice 23 sem mudanças no caller.
- Memória ainda controlada e previsível: pico ~540 MB para 3 variáveis
  simultâneas, dentro do orçamento do worker (~1,6 GB).

### Negativas

- Pico de memória maior por execução individual (de ~50 MB durante a
  iteração lazy para ~180 MB por variável materializada). Aceitável
  dado o orçamento disponível.
- O iterator deixa de ser "lazy" no sentido estrito do dask. Continua
  semanticamente um iterador (produz uma tupla por município por vez
  para o consumidor), mas a entrada é totalmente materializada antes
  da primeira tupla.

## Referências

- Bench: `scripts/bench_iterar_municipios.py` (Slice 24).
- Profiling: `tests/perf/profile_slice_23.txt` (Slice 24).
- ADR-013 — Pipeline streaming (premissa original; ver nota de
  correção no topo).
- ADR-015 — Filtro `municipios_alvo` (preservado e potencializado).
