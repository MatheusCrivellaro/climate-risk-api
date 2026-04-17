# Operação da Fila de Jobs

A fila de jobs é uma tabela SQLite (`job`) processada por um **processo
worker dedicado**. A decisão arquitetural está em [ADR-004](adrs/0004-fila-jobs-sqlite.md).

## Iniciar o worker

Em um terminal separado da API:

```bash
uv run climate-risk-worker
```

O worker faz polling na tabela `job` a cada `CLIMATE_RISK_WORKER_POLL_INTERVAL_SECONDS`
segundos (default `2`) e emite heartbeat a cada `CLIMATE_RISK_WORKER_HEARTBEAT_SECONDS`
(default `30`).

## Monitorar jobs

Todos via API HTTP:

| Operação | Endpoint |
|---|---|
| Listar jobs (com filtros) | `GET /jobs?status=<s>&tipo=<t>&limit=<n>&offset=<o>` |
| Obter job específico | `GET /jobs/{id}` |
| Reprocessar job que falhou | `POST /jobs/{id}/retry` |

Exemplos comuns:

- `GET /jobs?status=pending` — jobs aguardando processamento.
- `GET /jobs?status=running` — jobs em execução agora.
- `GET /jobs?status=failed` — jobs que falharam após todas as tentativas.

## Reprocessar um job falho

```bash
curl -X POST http://localhost:8000/jobs/<job_id>/retry
```

Só é permitido quando `status == "failed"`. Em qualquer outro estado, a API
responde `409 Conflict` (`type=/errors/job-estado-invalido`). A operação reseta
`tentativas=0`, limpa `erro`/`concluido_em`/`proxima_tentativa_em` e devolve
o job para `pending`; o worker pega no próximo ciclo.

## Recuperação automática (jobs zumbis)

Antes de cada tentativa de aquisição, o worker executa um sweep:

- Critério: `status='running'` e `heartbeat < now - (heartbeat_seconds × 3)`.
- Ação: devolve ao status `pending`, incrementando `tentativas`.

Isso cobre o caso do worker ter morrido no meio de um job; a morte conta
como uma tentativa consumida para evitar loops de crash infinitos.

## Política de retry

Definida em ADR-004:

| Tentativa | Backoff |
|---|---|
| 1 (após 1ª falha) | `now + 2s` |
| 2 (após 2ª falha) | `now + 8s` |
| 3 (após 3ª falha) | `now + 30s` |
| ≥ `max_tentativas` | status final `failed` |

O Worker é quem aplica a política; a `FilaJobs` apenas executa o que lhe
pedem (ver docstring de `FilaJobs.concluir_com_falha`).

## Tipos de job suportados (Slice 5)

- `noop` — utilitário de diagnóstico/smoke-test. Aceita payload
  `{"duracao_segundos": float, "falhar": bool, "mensagem_erro": str}`.

O tipo `processar_cordex` entra no Slice 6.

## Limitações conhecidas (ADR-004)

- **Worker único**: não rodar dois processos de worker simultaneamente
  contra o mesmo arquivo SQLite. A atomicidade de aquisição é garantida
  no nível do SQL (ver `FilaSQLite.adquirir_proximo`), mas a decisão
  arquitetural do MVP é manter um único worker.
- **Polling fixo**: não há push. Intervalo mínimo recomendado de 1
  segundo. Para jobs urgentes, reduza o intervalo via `.env` (aumenta
  CPU em idle).
- **Sem priorização**: jobs são processados em FIFO por `criado_em`.
- **Sem deadline por tipo**: a Slice 5 não implementa timeouts; o
  Worker só cancela um job se o processo for encerrado.

## Variáveis de ambiente relevantes

| Variável | Default | Descrição |
|---|---|---|
| `CLIMATE_RISK_WORKER_POLL_INTERVAL_SECONDS` | `2` | Intervalo de polling quando fila vazia. |
| `CLIMATE_RISK_WORKER_HEARTBEAT_SECONDS` | `30` | Intervalo entre atualizações de heartbeat. |
| `CLIMATE_RISK_DATABASE_URL` | `sqlite+aiosqlite:///./climate_risk.db` | URL do banco compartilhado entre API e worker. |
| `CLIMATE_RISK_LOG_LEVEL` | `INFO` | Nível de log do worker. |
