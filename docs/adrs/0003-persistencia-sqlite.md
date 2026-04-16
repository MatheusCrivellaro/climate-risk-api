# ADR-003 — Persistência em SQLite com Plano de Migração para PostgreSQL

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

Persistência de resultados foi confirmada como requisito. PostgreSQL não é instalável no ambiente corporativo atual. É necessária uma solução que:

- funcione sem instalação de serviços;
- seja SQL real (para viabilizar migração futura);
- suporte o volume previsto de resultados em formato longo.

## Decisão

Usar **SQLite** como banco, acessado via SQLAlchemy async (driver `aiosqlite`). Modo WAL habilitado para permitir leituras concorrentes durante escrita. Migrations gerenciadas pelo **Alembic**. Operações geoespaciais executadas na aplicação via Shapely/numpy (sem SpatiaLite/PostGIS no MVP).

## Consequências

**Positivas:**
- Zero instalação — SQLite é embutido no Python.
- Migração para PostgreSQL é majoritariamente configuração: trocar connection string e revisar tipos específicos (ex.: `JSON`).
- Arquivo único facilita backup, inspeção e versionamento de dados de teste.
- Alembic garante schema reprodutível e evolutivo.

**Negativas:**
- SQLite serializa escritas (um writer por vez). Para o caso atual (worker único de processamento), não é problema.
- Sem tipos geográficos nativos — operações geoespaciais mais pesadas ficam na aplicação.
- Tipos SQLite são dinâmicos (*type affinity*) — requer disciplina para não criar dependências implícitas do comportamento SQLite que quebrariam no PostgreSQL.
