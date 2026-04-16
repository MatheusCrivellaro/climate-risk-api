# Documentação do Projeto

Esta pasta contém toda a documentação de análise, decisão arquitetural e planejamento do projeto **Climate Risk API**.

## Índice

### Análise e diagnóstico
- [Análise inicial](analise-inicial.md) — Inventário do código legado, dependências, dívida técnica.
- [Diagnóstico consolidado](diagnostico-consolidado.md) — Síntese dos achados de análise + riscos.

### Decisões arquiteturais (ADRs)
- [ADR-001 — Consolidação do núcleo de cálculo climático](adrs/0001-consolidacao-nucleo.md)
- [ADR-002 — FastAPI + SQLAlchemy async](adrs/0002-fastapi-sqlalchemy.md)
- [ADR-003 — Persistência em SQLite](adrs/0003-persistencia-sqlite.md)
- [ADR-004 — Fila de jobs em SQLite](adrs/0004-fila-jobs-sqlite.md)
- [ADR-005 — Arquitetura em camadas explícitas](adrs/0005-camadas-explicitas.md)
- [ADR-006 — Execução nativa sem containerização](adrs/0006-execucao-nativa.md)
- [ADR-007 — Detecção de unidade de precipitação](adrs/0007-deteccao-unidade.md)

### Desenho e planejamento
- [Desenho da API e arquitetura alvo](desenho-api.md)
- [Plano de refatoração incremental](plano-refatoracao.md)

## Convenções

- Todos os ADRs seguem o formato: **Título · Contexto · Decisão · Consequências · Status**.
- Documentos são versionados junto com o código — mudanças arquiteturais exigem atualização da documentação correspondente no mesmo PR.
- Novos ADRs recebem o próximo número sequencial disponível e nunca são deletados, apenas marcados como **Superseded** quando substituídos.
