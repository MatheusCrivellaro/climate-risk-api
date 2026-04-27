# ADR-010 — Leitura lazy de NetCDF para evitar estouro de memória em séries longas

**Status:** Aceito.
**Data:** 2026-04-25.

## Contexto

A Slice 17 expôs o pipeline de estresse hídrico ao caso real: cinco
arquivos NetCDF por variável (≈5 anos cada), três variáveis (`pr`, `tas`,
`evspsbl`) e dois cenários (`rcp45`, `rcp85`) processados em paralelo. O
método `LeitorCordexMultiVariavel.abrir_de_pastas` materializava cada
variável inteira em RAM via `.load()` antes de devolver o lote para o
agregador, derrubando o worker em máquinas com menos de ~16 GB livres.

Diagnóstico do estouro:

```python
da_pr = da_pr.sel(time=tempo_comum).load()    # ~5 GB em RAM
da_tas = da_tas.sel(time=tempo_comum).load()  # +5 GB
da_evap = da_evap.sel(time=tempo_comum).load()# +5 GB → 15 GB pico por execução
```

E adicionalmente, o `xr.concat([...], dim="time", join="outer")` seguido
de `concatenado.isel(time=ordem)` (reordenação por tempo) materializava
todos os arquivos da variável de uma vez antes mesmo do `.load()` final.

## Decisão

Trocar a leitura por `xr.open_mfdataset` com chunks dask, e remover os
`.load()` posteriores. A materialização passa a ocorrer apenas no
agregador espacial, ao iterar município a município (que já operava
sobre `np.asarray(dados.values)` por variável de cada vez).

Pontos da implementação:

- **Pré-scan de cenário**: cada arquivo é aberto rapidamente com
  `xr.open_dataset(..., decode_times=False)` apenas para inspecionar
  `ds.attrs["experiment_id"]` (ou regex no nome). Isso isola a validação
  de cenário do caminho lazy de leitura de dados.
- **Abertura lazy**: `xr.open_mfdataset(arquivos, combine="nested",
  concat_dim="time", chunks={"time": 365}, ...)`. `combine="nested"`
  preserva a ordem dos arquivos (que já chegam ordenados por nome) sem
  exigir que os tempos sejam disjuntos; o dedup de timestamps duplicados
  acontece via `da.isel(time=np.flatnonzero(~mask))`, que é lazy em dask.
- **Conversão de unidade e calendário**: operações aritméticas
  (`da * 86400.0`, `da - 273.15`) e `convert_calendar()` preservam chunks
  dask em todas as versões testadas de xarray (≥2024.6).
- **Sem `.load()`**: `abrir_de_pastas` devolve `DadosClimaticosMultiVariaveis`
  com os três `DataArray` em chunks. O agregador é o único ponto onde
  `.values` força computação — mas o faz por variável, e descarta a
  numpy array antes da próxima.

## Alternativas descartadas

### Manter `.load()` mas processar variável a variável serialmente

Reduziria o pico de 15 GB para ~5 GB, mas continuaria estourando em
máquinas modestas (e o futuro pipeline com mais anos por variável
estouraria de novo). Não resolve o problema; só adia.

### Reduzir `chunks={"time": 365}` para `chunks={"time": 30}` (chunk mensal)

Mais granular, mas adiciona overhead de dask (mais tasks, mais agendamento).
Para grades CORDEX típicas (~390×355), 365 dias por chunk dão chunks
de ~200 MB, que dask processa eficientemente. Mensal não traz benefício.

### Migrar o agregador para iterar por chunk dask

Refatoração maior. Hoje o agregador faz `np.asarray(dados.values)` uma
vez por variável e depois itera municípios sobre numpy. Isso já é
suficiente para o budget de memória atual; iterar por chunk dask
reduziria mais o pico mas adiciona complexidade. Fica como evolução
futura se a grade ou a janela temporal crescerem ainda mais.

## Consequências

**Positivas:**

- Pico de RAM cai de ~15 GB para ~5 GB (uma materialização por variável,
  no agregador).
- Worker estável em máquinas com 8 GB livres rodando dois cenários em
  paralelo.
- Caminho de leitura é o mesmo padrão recomendado pela documentação
  oficial do xarray para multi-arquivo.

**Negativas:**

- Adiciona `dask` como dependência direta. xarray declara dask como
  *optional*; o instalador padrão do `xarray>=2024.6` não puxa dask.
- O ciclo de vida dos *file handles* fica acoplado ao GC do Python:
  enquanto o `DataArray` lazy estiver referenciado, os arquivos `.nc`
  permanecem abertos. Aceitável porque o handler do worker consome o
  lote inteiro numa única invocação.
- Calendários `noleap`/`360_day` continuam sendo convertidos via
  `convert_calendar` — operação lazy mas que adiciona NaN no dia 29/02.
  Comportamento já documentado em ADR-009.

## Plano de revisão

Reavaliar quando:

- A janela temporal por execução passar de 30 anos (hoje 25), ou a
  grade espacial subir significativamente — pode ser hora de iterar por
  chunk dask no agregador.
- xarray distribuir dask como *required*, momento em que a nota sobre a
  dependência explícita aqui pode ser removida.
- Se aparecer regressão do tipo "arquivo não encontrado" durante uso
  do `DataArray` lazy, revisitar a estratégia de manter file handles
  abertos (talvez forçar `.persist()` antes de devolver o lote).
