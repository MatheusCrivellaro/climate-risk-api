# CLAUDE.md — backend

Documento vivo para contextualizar novas sessões do Claude Code no backend
do Climate Risk API. Para o frontend, ver `frontend/CLAUDE.md`.

## Pipelines e formatos de resultados

O backend hoje expõe **dois** pipelines de cálculo independentes:

1. **Precipitação extrema** (Slices 1–12) — persiste em `resultado_indice`
   no formato **long** (uma linha por `nome_indice`). Consultas em
   `/api/resultados`, `/api/resultados/agregados`, `/api/resultados/stats`.
2. **Estresse hídrico** (Slices 13–15) — persiste em
   `resultado_estresse_hidrico` no formato **wide** (uma linha agrega
   frequência e intensidade como colunas separadas, indexada por
   `(execucao_id, municipio_id, ano)`). Consultas em
   `/api/resultados/estresse-hidrico`.

### Convenção de resultados

O pipeline de estresse hídrico usa formato **wide** (frequência e
intensidade como colunas), **não** o formato *long* do `ResultadoIndice`.
Para adicionar novos indicadores ao pipeline, alterar o schema da tabela
`resultado_estresse_hidrico` via Alembic migration.

Motivos da decisão:

- Frequência e intensidade são sempre consumidas em par.
- Consultas agregadas (ex.: médias ponderadas) ficam triviais em wide.
- Mantém o pipeline de precipitação extrema inalterado.

## Página `/estudo/` — modo lote

Página `/estudo/` opera no modo "lote": aceita 6 pastas (3 variáveis × 2
cenários) e cria 2 execuções por submit, via
`POST /api/execucoes/estresse-hidrico/em-lote`. O endpoint antigo
`POST /api/execucoes/estresse-hidrico` (arquivo único) continua existindo
para uso programático com arquivo único.

O leitor (`LeitorCordexMultiVariavel`) tem dois métodos:

- `abrir(pr, tas, evap)` — três arquivos individuais (Slice 13).
- `abrir_de_pastas(pasta_pr, pasta_tas, pasta_evap, cenario_esperado)` —
  concatena todos os `.nc` de cada pasta no eixo temporal e valida que
  cada arquivo declara o `cenario_esperado` (Slice 17).

## Convenções

- Arquitetura hexagonal com camadas explícitas (ADR-005): `domain`,
  `application`, `infrastructure`, `interfaces`.
- Timestamps persistidos como ISO 8601 UTC em `String(32)` (ADR-003).
- IDs gerados pela aplicação com prefixos: `exec_`, `job_`, `res_`, `reh_`.
- Português no domínio; inglês em identificadores técnicos.
