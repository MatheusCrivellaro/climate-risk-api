# Diagnóstico Consolidado

## 1. Visão Geral

O repositório atual é, na prática, um **pipeline batch de avaliação de risco climático para fornecedores**, composto por quatro estágios acoplados via arquivos intermediários no filesystem. A documentação existente cobre apenas um desses estágios, o que gera descompasso significativo entre o sistema real e sua descrição formal.

O objetivo desta transformação é consolidar esses estágios em um **sistema único, servido como API HTTP local**, com arquitetura em camadas explícitas, persistência em SQLite e processamento assíncrono via fila nativa. O comportamento funcional será preservado; melhorias estruturais não devem introduzir regressões.

## 2. Estado Atual — Síntese

### 2.1 Composição do repositório

| Arquivo | Função | Situação |
|---|---|---|
| `cordex_pr_freq_intensity.py` | Pipeline CORDEX em grade completa (BBOX) | Ativo, documentado |
| `gera_pontos_fornecedores.py` | Pipeline CORDEX em pontos exatos | Ativo, não documentado |
| `gera_base_fornecedores.py` | Geocodificação de cidades via IBGE | Ativo, não documentado |
| `locais_faltantes_fornecedores.ipynb` | Reconciliação fornecedores × grade | Exploratório |
| `teste.py` | — | Código morto |
| `code_doc.docx` | Documentação parcial | Desatualizada |

### 2.2 Fluxo de dados atual

```mermaid
