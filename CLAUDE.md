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

### Modo lote (Slice 17)

Página `/estudo/` opera no modo "lote": aceita 6 pastas e cria 2 execuções
por submit. Endpoint antigo `/api/execucoes/estresse-hidrico` continua
existindo para uso programático com arquivo único.

### Definição de intensidade (Slice 19)

A partir da Slice 19, `intensidade_mm_dia` representa a **média** do
déficit hídrico nos dias secos quentes (mm/dia), calculada como
``soma(deficit nos dias secos quentes) / frequencia_dias_secos_quentes``.
Quando `frequencia_dias_secos_quentes == 0`, a intensidade é definida
como `0.0` por convenção (não NaN). A definição anterior (soma total em
mm) foi descartada — ver ADR-011. A coluna no banco passou a se chamar
`intensidade_mm_dia` (migration `5c2d8a17b9f4`); a tabela
`resultado_estresse_hidrico` foi zerada na migração porque os valores
antigos não eram convertíveis sem recalcular.

### Memória no leitor multi-variável (Slice 18)

A leitura de pastas com múltiplos `.nc` usa `xr.open_mfdataset` com chunks
dask (`chunks={"time": 365}`). Materialização em RAM acontece só durante
agregação espacial, município a município. Não usar `.load()` nem
`isel(time=ordem)` em DataArrays grandes — força materialização total.
Ver ADR-010.

### Resiliência da fila de jobs (Slice 18)

Jobs que falham 3 vezes consecutivas são marcados como `failed`
definitivamente, não retornam para `pending` (constante `MAX_TENTATIVAS`
em `infrastructure/fila/fila_sqlite.py`). O método `concluir_com_falha`
usa sessão limpa (via `async_sessionmaker` injetado) para evitar conflito
com a sessão da task de heartbeat — sem esse fix, falhas geravam o erro
SQLAlchemy `"This session is provisioning a new connection; concurrent
operations are not permitted"` e entravam em loop de retry.

### Endpoints da Slice 20.1

- `GET /api/fs/listar`: browser de pastas no servidor. Requer
  `CLIMATE_RISK_FS_RAIZ`. Path traversal mitigado via
  `resolve(strict=True) + relative_to(raiz)`.
- `GET /api/resultados/estresse-hidrico/export`: exporta resultados em
  CSV/XLSX/JSON. Limite de 200.000 linhas (constante
  `LIMITE_LINHAS_EXPORT` em `interfaces/rotas/estresse_hidrico.py`).

### Interface principal

`/app/` desativado a partir da Slice 20 (ADR-012); `/estudo/` é a única
interface visível. O código React permanece em `frontend/` para reativação
futura, mas o mount foi removido de `interfaces/app.py`.

## Convenções

- Arquitetura hexagonal com camadas explícitas (ADR-005): `domain`,
  `application`, `infrastructure`, `interfaces`.
- Timestamps persistidos como ISO 8601 UTC em `String(32)` (ADR-003).
- IDs gerados pela aplicação com prefixos: `exec_`, `job_`, `res_`, `reh_`.
- Português no domínio; inglês em identificadores técnicos.
