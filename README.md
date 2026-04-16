# climate-risk-api

API para cálculo de índices de frequência e intensidade de precipitação sobre
dados CORDEX. Consome arquivos NetCDF, persiste resultados em SQLite e expõe
endpoints HTTP para geocodificação, cadastro de fornecedores, execuções de
processamento de grade, cálculo por pontos e consulta de resultados.

A arquitetura segue quatro camadas explícitas (`domain`, `application`,
`infrastructure`, `interfaces`), com execução nativa (sem Docker) gerenciada
pelo `uv`. O Slice 0 entrega apenas a fundação do projeto — endpoints de
negócio chegam a partir do Slice 4. Consulte `docs/plano-refatoracao.md` para
o roadmap completo.

## Pré-requisitos

- **Python 3.12+**
- **Bibliotecas de sistema HDF5/netCDF** (necessárias para `netcdf4` e `xarray`):
  - Debian/Ubuntu: `sudo apt-get install -y libhdf5-dev libnetcdf-dev`
  - macOS (Homebrew): `brew install hdf5 netcdf`
  - Windows (Chocolatey): `choco install hdf5 netcdf`
  - Windows (winget): `winget install --id=HDFGroup.HDF5` (netCDF normalmente
    chega via wheel pré-compilado do PyPI; veja abaixo).
- **uv** como gerenciador de dependências e runner:
  `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Bootstrap (5 passos)

```bash
# 1. Instalar o uv (somente uma vez).
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Sincronizar dependências (cria .venv automaticamente).
uv sync --all-extras

# 3. Copiar o template de configuração.
cp .env.example .env

# 4. Aplicar migrations (cria climate_risk.db).
uv run alembic upgrade head

# 5. Rodar a API (http://localhost:8000/health deve retornar {"status":"ok"}).
uv run climate-risk-api
```

## Rodar testes

```bash
uv run pytest
```

Também é possível executar cada verificação isoladamente:

```bash
uv run ruff check
uv run ruff format --check
uv run mypy src/
```

## Documentação

- `docs/plano-refatoracao.md` — slices e critérios de conclusão.
- `docs/desenho-api.md` — endpoints, contratos e estrutura de pastas.
- `docs/adrs/` — decisões arquiteturais.

Para referência do código antigo, ver `CODE_ANTIGO_REFERENCIA.md`.
