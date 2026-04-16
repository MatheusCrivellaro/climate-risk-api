# ADR-004 — Fila de Jobs em SQLite com Worker Dedicado

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

Processamento de arquivos `.nc` CORDEX é I/O + CPU intensivo, podendo durar de minutos a dezenas de minutos por arquivo. Executar síncrono em request HTTP é inviável.

ARQ/Celery foram considerados mas exigem Redis, que não é instalável no ambiente atual. FastAPI `BackgroundTasks` é insuficiente porque jobs longos morrem com reinício do processo da API.

## Decisão

Implementar fila de jobs como **tabela SQLite** (`job`) com colunas mínimas:
- `id`, `tipo`, `payload` (JSON)
- `status` (pending, running, completed, failed, canceled)
- `tentativas`, `max_tentativas`
- `criado_em`, `iniciado_em`, `concluido_em`, `heartbeat`
- `erro`, `proxima_tentativa_em`

Worker é processo Python separado que faz *polling* com intervalo configurável (ex.: 2s), usando transações com `UPDATE ... RETURNING` para aquisição atômica.

Abstração via interface `JobQueue` permite trocar implementação depois.

**Política de retry:** 3 tentativas, backoff exponencial 2s → 8s → 30s.
**Timeouts por tipo:** geocodificação 5min, processamento CORDEX 2h.
**Jobs zumbis:** status `running` sem heartbeat recente voltam a `pending`.

## Consequências

**Positivas:**
- Zero infraestrutura extra — aproveita o SQLite já decidido.
- Inspeção trivial: jobs são linhas SQL.
- Fácil debugging (jobs permanecem no banco após conclusão).
- Migração futura para ARQ/Celery isola-se atrás da interface `JobQueue`.

**Negativas:**
- Polling consome ciclos mesmo sem jobs.
- Implementação manual de recursos que ARQ daria de graça: retry com backoff, priorização, deadline, dashboard.
- Concorrência entre múltiplos workers sobre o mesmo SQLite exige cuidado — MVP terá **um worker único**, documentado como limitação.
- Em caso de crash do worker durante execução de job, o estado "em andamento" precisa ser recuperável via timeout que devolve o job à fila.
