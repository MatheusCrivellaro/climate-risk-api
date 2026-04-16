# ADR-006 — Execução Nativa Sem Containerização

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

Ambiente corporativo não permite Docker nem Docker Compose. A aplicação será executada nativamente, com processos Python iniciados diretamente (API e worker).

## Decisão

Distribuir como projeto Python gerenciado por **uv**, com `pyproject.toml` declarando todas as dependências. Execução via comandos explícitos:

- `uv run uvicorn climate_risk.cli.api:app` (API)
- `uv run python -m climate_risk.cli.worker` (worker)

Instruções de bootstrap no README, incluindo pré-requisitos de sistema não instaláveis via pip (notadamente HDF5/netCDF libs).

## Consequências

**Positivas:**
- Sem dependência de runtime de containers.
- Desenvolvedor itera mais rápido (sem rebuild de imagens).
- Reprodutibilidade do ambiente Python garantida por `uv.lock`.

**Negativas:**
- Dependências binárias de sistema (HDF5) ficam fora do `uv.lock`. Se não estiverem instaladas, instalação manual é requerida. Mitigação: README com instruções por sistema operacional.
- Deploy em múltiplas máquinas exige repetir o setup manualmente.
- Sem isolamento de ambiente entre projetos (`uv` ajuda via venv, mas não isola libs C).
