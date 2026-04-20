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

Swagger/OpenAPI completo em `/docs`.

| Grupo | Endpoints |
|---|---|
| Health | `GET /health` (liveness), `GET /health/ready` (banco + migrações `head`). |
| Cálculo síncrono | `POST /calculos/pontos` — até 100 pontos; pura, não persiste. |
| Execuções assíncronas | `POST /execucoes` (CORDEX em grade), `POST /execucoes/pontos` (lote grande de pontos), `GET /execucoes`, `GET /execucoes/{id}`, `DELETE /execucoes/{id}`. |
| Jobs (fila) | `GET /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/retry`. |
| Resultados | `GET /resultados`, `GET /resultados/agregados`, `GET /resultados/stats`. |
| Geocodificação | `POST /localizacoes/geocodificar`, `POST /localizacoes/pontos`. |
| Fornecedores | `POST/GET/DELETE /fornecedores`, `POST /fornecedores/importar`. |
| Cobertura | `POST /cobertura/fornecedores`. |
| Admin | `POST /admin/ibge/refresh`, `GET /admin/stats`. |

Erros seguem RFC 7807 (`application/problem+json`) — ver
`docs/desenho-api.md` para a tabela completa de códigos.

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
