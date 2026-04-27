# climate-risk-api

API para cálculo de índices anuais de frequência e intensidade de
precipitação sobre dados CORDEX (NetCDF). Expõe um backend FastAPI que
abrange todo o fluxo originalmente implementado em scripts sparse:
geocodificação de localizações via IBGE, cadastro/importação de
fornecedores, cobertura climática, processamento de cenários inteiros em
grade, cálculo síncrono por pontos e consulta/agregação dos resultados.

A arquitetura segue quatro camadas explícitas — `domain`, `application`,
`infrastructure`, `interfaces` (ver [ADR-005](docs/adrs/0005-camadas-explicitas.md))
— com execução nativa gerenciada pelo [`uv`](https://docs.astral.sh/uv/)
e persistência em SQLite ([ADR-003](docs/adrs/0003-persistencia-sqlite.md)).
A fila de jobs também é SQLite ([ADR-004](docs/adrs/0004-fila-jobs-sqlite.md))
e roda em um worker dedicado.

## Pré-requisitos

- **Python 3.12+**
- **Bibliotecas HDF5/netCDF** (necessárias para `netcdf4` e `xarray`):
  - Debian/Ubuntu: `sudo apt-get install -y libhdf5-dev libnetcdf-dev`
  - macOS (Homebrew): `brew install hdf5 netcdf`
  - Windows: instalar HDF5 (`winget install HDFGroup.HDF5`); `netcdf4`
    normalmente chega via wheel pré-compilado do PyPI.
- **uv**: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Bootstrap

```bash
# 1. Instalar o uv (somente uma vez).
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Sincronizar dependências (cria .venv automaticamente).
uv sync

# 3. Copiar o template de configuração.
cp .env.example .env

# 4. Aplicar migrations (cria climate_risk.db).
uv run alembic upgrade head

# 5. Rodar a API (Swagger em http://localhost:8000/docs).
uv run climate-risk-api
```

Em outro terminal, suba o worker para processar as execuções assíncronas:

```bash
uv run climate-risk-worker
```

## Endpoints (visão rápida)

Swagger/OpenAPI completo em `/docs`. Todas as rotas da API ficam sob o
prefixo `/api/` (o root `/` é reservado para quando o frontend estiver
montado em `/app/`).

| Grupo | Endpoints |
|---|---|
| Health | `GET /api/health` (liveness), `GET /api/health/ready` (banco + migrações `head`). |
| Cálculo síncrono | `POST /api/calculos/pontos` — até 100 pontos; pura, não persiste. |
| Execuções assíncronas | `POST /api/execucoes` (CORDEX em grade), `POST /api/execucoes/pontos` (lote grande de pontos), `GET /api/execucoes`, `GET /api/execucoes/{id}`, `DELETE /api/execucoes/{id}`. |
| Jobs (fila) | `GET /api/jobs`, `GET /api/jobs/{id}`, `POST /api/jobs/{id}/retry`. |
| Resultados | `GET /api/resultados`, `GET /api/resultados/agregados`, `GET /api/resultados/stats`, `GET /api/resultados/estresse-hidrico`, `GET /api/resultados/estresse-hidrico/export` (CSV/XLSX/JSON). |
| Filesystem | `GET /api/fs/listar` — browser de pastas (requer `CLIMATE_RISK_FS_RAIZ`). |
| Geocodificação | `POST /api/localizacoes/geocodificar`, `POST /api/localizacoes/pontos`. |
| Fornecedores | `POST/GET/DELETE /api/fornecedores`, `POST /api/fornecedores/importar`. |
| Cobertura | `POST /api/cobertura/fornecedores`. |
| Admin | `POST /api/admin/ibge/refresh`, `GET /api/admin/stats`. |

Erros seguem RFC 7807 (`application/problem+json`) — ver
`docs/desenho-api.md` para a tabela completa de códigos.

## Interface

A interface principal está em `/estudo/`, focada exclusivamente no pipeline
de estresse hídrico. É HTML/CSS/JS puro, sem dependências externas além de
Chart.js (CDN), e funciona assim que o backend subir.

Acesso: `http://localhost:8000/estudo/`

A interface React em `/app/` foi desativada — o código permanece em
`frontend/` para reativação futura, mas não está montada na aplicação. Os
endpoints `/api/*` continuam todos funcionais para uso programático.

A página `/estudo/` opera no modo **lote**: aceita 6 pastas (3 variáveis x 2
cenários) num único formulário e cria duas execuções (`rcp45` + `rcp85`)
por submit. Consome o endpoint `POST /api/execucoes/estresse-hidrico/em-lote`
(Slice 17), que aceita pastas em vez de arquivos individuais — todos os
`.nc` de cada pasta são concatenados temporalmente pelo handler.

A tabela de resultados expõe duas métricas por município/ano:

- **Frequência (dias)** — número de dias secos quentes (`pr ≤ limiar_pr`
  **e** `tas ≥ limiar_tas`).
- **Intensidade (mm/dia)** — **média** do déficit hídrico (`evap - pr`)
  por dia seco quente do ano. Quando a frequência é zero, intensidade
  vale `0.0` por convenção (Slice 19, ADR-011 — antes era a soma anual).

O endpoint antigo `POST /api/execucoes/estresse-hidrico` (arquivo único)
continua existindo para uso programático.

### Configuração necessária

- `CLIMATE_RISK_SHAPEFILE_MUN_PATH`: caminho para o shapefile de municípios
  IBGE.
- `CLIMATE_RISK_FS_RAIZ`: pasta raiz a partir da qual o browser de pastas
  pode navegar (necessária para a feature de seleção visual de pastas em
  `/estudo/`).

## Frontend

O projeto convive com um frontend React/Vite/TypeScript em `frontend/`. A
partir da Slice 20 ele **não é montado pelo backend** — o código permanece
disponível para reativação futura, mas a única interface visível é
`/estudo/` (HTML/CSS/JS puro). Para reativar `/app/`, descomentar o trecho
sinalizado em `src/climate_risk/interfaces/app.py`.

### Estrutura

```
climate-risk-api/
├── src/climate_risk/        Backend (FastAPI + worker)
├── tests/                   Testes Python (pytest)
├── frontend/                Frontend (React + Vite)
│   ├── src/                 Código TypeScript
│   ├── dist/                Build estático (gerado, não versionado)
│   └── package.json
├── pyproject.toml
└── README.md
```

### Modo dev (três terminais)

Recomendado para desenvolvimento diário. Permite HMR do Vite sem reiniciar
nada no backend.

```bash
# Terminal 1 — API em http://localhost:8000
uv run climate-risk-api

# Terminal 2 — worker de jobs
uv run climate-risk-worker

# Terminal 3 — frontend em http://localhost:5173
cd frontend
pnpm install           # apenas uma vez
pnpm dev
```

O Vite dev server faz proxy de `/api` para `http://localhost:8000`, então
o frontend sempre consome `/api/...` com mesma-origem e não há CORS.

Abra `http://localhost:5173/app/` no browser. O Swagger continua em
`http://localhost:8000/docs`.

### URLs relevantes

| URL                                  | O que responde                              |
| ------------------------------------ | ------------------------------------------- |
| `http://localhost:8000/estudo/`      | Interface HTML/CSS/JS puro (estresse hídrico). |
| `http://localhost:8000/api/...`      | Rotas da API.                               |
| `http://localhost:8000/docs`         | Swagger UI.                                 |
| `http://localhost:8000/openapi.json` | Contrato OpenAPI cru.                       |

`/app/` retorna 404 — o mount foi desativado na Slice 20 (ADR-012). Para
reativá-lo, descomentar o trecho identificado em
`src/climate_risk/interfaces/app.py`.

### Regerando tipos quando o OpenAPI muda

O frontend consome `schema.d.ts` gerado a partir do OpenAPI do backend.
Quando rotas ou schemas mudarem:

```bash
# Com o backend rodando em http://localhost:8000:
cd frontend
pnpm gen:types
```

O script grava `frontend/src/api/schema.d.ts`. Commite junto com a
mudança do backend para manter frontend e contrato em sincronia.

## Testes e qualidade

```bash
# Suíte completa (unit + integration + e2e) com cobertura.
uv run pytest

# Checks isolados.
uv run ruff check
uv run ruff format --check
uv run mypy src/
```

Gate de cobertura: **≥90%** nas linhas de `src/climate_risk`. Testes que
dependem de fixtures pesadas (como o shapefile IBGE) ficam marcados com
`@pytest.mark.shapefile` e são pulados automaticamente quando o fixture
não está presente.

Checks do frontend (dentro de `frontend/`):

```bash
pnpm typecheck    # tsc --noEmit
pnpm lint         # eslint
pnpm test         # vitest (modo run)
pnpm build        # build de produção
```

## Documentação

- [`docs/plano-refatoracao.md`](docs/plano-refatoracao.md) — roadmap das
  12 slices (MVP completo) e critérios de aceitação.
- [`docs/desenho-api.md`](docs/desenho-api.md) — endpoints, contratos,
  estrutura de diretórios, fluxos principais.
- [`docs/operacoes.md`](docs/operacoes.md) — worker, fila, recuperação
  de jobs zumbis, troubleshooting.
- [`docs/divida-tecnica.md`](docs/divida-tecnica.md) — débitos aceitos
  e resolvidos.
- [`docs/adrs/`](docs/adrs/) — decisões arquiteturais (ADR-001…012).

## Histórico

O projeto foi refatorado de um conjunto de scripts/notebooks Python
(gera_pontos_fornecedores.py, cordex_pr_freq_intensity.py,
locais_faltantes_fornecedores.ipynb) para uma API hexagonal. O código
legado foi removido na Slice 12; a paridade bit-a-bit com ele está
congelada como *golden baseline* em
`tests/integration/test_paridade_legacy.py` e
`tests/fixtures/baselines/`.
