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
| Execuções assíncronas | `POST /api/execucoes` (CORDEX em grade), `POST /api/execucoes/pontos` (lote grande de pontos), `POST /api/execucoes/estresse-hidrico` (3 arquivos), `POST /api/execucoes/estresse-hidrico/em-lote` (6 pastas → rcp45 + rcp85), `GET /api/execucoes`, `GET /api/execucoes/{id}`, `DELETE /api/execucoes/{id}`. |
| Jobs (fila) | `GET /api/jobs`, `GET /api/jobs/{id}`, `POST /api/jobs/{id}/retry`. |
| Resultados | `GET /api/resultados`, `GET /api/resultados/agregados`, `GET /api/resultados/stats`. |
| Geocodificação | `POST /api/localizacoes/geocodificar`, `POST /api/localizacoes/pontos`. |
| Fornecedores | `POST/GET/DELETE /api/fornecedores`, `POST /api/fornecedores/importar`. |
| Cobertura | `POST /api/cobertura/fornecedores`. |
| Admin | `POST /api/admin/ibge/refresh`, `GET /api/admin/stats`. |

Erros seguem RFC 7807 (`application/problem+json`) — ver
`docs/desenho-api.md` para a tabela completa de códigos.

## Interfaces disponíveis

O projeto oferece **duas interfaces web** servidas pelo mesmo backend:

### `/app/` — Interface completa (React)

Painel administrativo completo, com dashboard, múltiplas execuções,
consultas ricas, CRUD de fornecedores, geocodificação e cobertura.

Requer build: `cd frontend && pnpm install && pnpm build`.

### `/estudo/` — Interface simplificada (HTML puro)

Página única focada **exclusivamente** no pipeline de estresse hídrico.
Aceita 6 pastas (3 variáveis × 2 cenários: `rcp45` e `rcp85`) e cria as
duas execuções num único clique via
`POST /api/execucoes/estresse-hidrico/em-lote`. Cada pasta pode conter
múltiplos `.nc` que são concatenados no eixo temporal pelo backend.
HTML + CSS + JS vanilla, sem build step.

Ideal para demos, testes rápidos e uso sem instalar Node. Não requer
build — funciona assim que o backend subir.

Acesso: `http://localhost:8000/estudo/`.

## Frontend

O projeto convive com um frontend React/Vite/TypeScript em `frontend/`.
Backend e frontend compartilham o mesmo repositório (monorepo simples) e,
em produção, a mesma origem — o FastAPI serve o build estático em `/app/`
e as rotas da API vivem em `/api/`.

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

### Modo demo integrada (build + backend)

Útil para reproduzir o comportamento de produção localmente (um único
processo servindo tudo).

```bash
# 1. Gerar o build estático do frontend.
cd frontend
pnpm install           # apenas uma vez
pnpm build             # cria frontend/dist/
cd ..

# 2. Subir o backend (que detecta frontend/dist/ e monta /app/).
uv run climate-risk-api
```

URLs relevantes:

| URL                               | O que responde                                |
| --------------------------------- | --------------------------------------------- |
| `http://localhost:8000/app/`      | Shell do frontend React (React Router toma dali). |
| `http://localhost:8000/estudo/`   | Interface simplificada HTML puro (estresse hídrico). |
| `http://localhost:8000/api/...`   | Rotas da API.                                 |
| `http://localhost:8000/docs`      | Swagger UI.                                   |
| `http://localhost:8000/openapi.json` | Contrato OpenAPI cru (fonte dos tipos).    |

Se `frontend/dist/` não existir quando o backend subir, `/app/*` responde
503 com um HTML explicando como rodar o build.

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
- [`docs/adrs/`](docs/adrs/) — decisões arquiteturais (ADR-001…007).

## Histórico

O projeto foi refatorado de um conjunto de scripts/notebooks Python
(gera_pontos_fornecedores.py, cordex_pr_freq_intensity.py,
locais_faltantes_fornecedores.ipynb) para uma API hexagonal. O código
legado foi removido na Slice 12; a paridade bit-a-bit com ele está
congelada como *golden baseline* em
`tests/integration/test_paridade_legacy.py` e
`tests/fixtures/baselines/`.
