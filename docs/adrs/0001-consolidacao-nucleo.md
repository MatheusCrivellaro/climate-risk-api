# ADR-001 — Consolidação do Núcleo de Cálculo Climático em Módulo Único

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

O repositório atual contém aproximadamente 15 funções duplicadas entre `cordex_pr_freq_intensity.py` e `gera_pontos_fornecedores.py`, incluindo `convert_pr_to_mm_per_day`, `annual_indices_for_point` / `annual_indices_for_series`, `compute_p95_threshold_per_cell` / `compute_p95_grid`, e utilidades geométricas.

As duas versões divergem em detalhes — por exemplo, `open_nc_multi` em `gera_pontos_fornecedores.py` é mais robusto (suporta `use_cftime`); a iteração por ano é mais idiomática na mesma versão. Manter duas cópias implica risco de divergência silenciosa: uma correção pode ser aplicada em um lado e esquecida no outro.

## Decisão

Unificar todo o cálculo climático (índices anuais, P95 por célula, conversão de unidade, normalização de coordenadas, seleção espacial) em um único módulo de domínio. A versão mais recente/robusta de cada função (tipicamente a presente em `gera_pontos_fornecedores.py`) prevalece.

As duas estratégias de seleção espacial (BBOX vs. pontos exatos) permanecem, mas atrás de uma interface comum.

## Consequências

**Positivas:**
- Eliminação de DT-01.
- Correções e melhorias aplicadas em um único lugar.
- Base clara para estender a evaporação e a outras variáveis.

**Negativas:**
- Refatoração inicial requer cuidado para não introduzir divergência numérica — mitigado pela baseline de regressão (risco R-02).
