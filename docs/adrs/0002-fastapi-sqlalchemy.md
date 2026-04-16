# ADR-002 — Adoção de FastAPI + SQLAlchemy Async

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

A transformação do sistema em API HTTP local exige escolha de framework. Requisitos: tipagem forte, documentação automática, suporte async (para I/O de banco e chamadas externas à API do IBGE), execução nativa sem dependências pesadas.

## Decisão

Usar **FastAPI** como framework web e **SQLAlchemy 2.x em modo async** como ORM. Pydantic v2 (já integrado ao FastAPI) para validação de entrada/saída. Driver `aiosqlite` para acesso assíncrono ao SQLite.

## Consequências

**Positivas:**
- Validação de tipos via Pydantic evita classe inteira de bugs.
- OpenAPI gerado automaticamente — substitui necessidade de documentação manual de endpoints.
- Modelo async natural para I/O paralelo (ex.: geocodificação de muitos municípios).
- SQLAlchemy abstrai o dialeto — migração futura para PostgreSQL requer mudança mínima.

**Negativas:**
- Async adiciona complexidade no código (precisa de `async def`, cuidado com bloqueios CPU-bound).
- O cálculo climático é CPU-bound e não se beneficia de async — precisará rodar em `run_in_executor` ou em worker separado.
- Curva de aprendizado se a equipe não estiver familiarizada com async.
