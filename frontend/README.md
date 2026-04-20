# Climate Risk UI

Frontend web do **Climate Risk API**. Interface sóbria para analistas técnicos
operarem execuções de cálculo de índices climáticos e stakeholders consultarem
resultados.

## Stack

- React 18 + Vite 5 + TypeScript (strict)
- TailwindCSS 3
- TanStack Query v5
- React Router v6
- Recharts
- `openapi-typescript` para gerar tipos a partir do OpenAPI do backend

## Requisitos

- Node 20+
- pnpm 9+ (ou npm 10+ como fallback)
- Backend rodando em `http://localhost:8000` (configurável via
  `VITE_API_BASE_URL`)

## Setup

```bash
cp .env.example .env
pnpm install
pnpm gen:types   # gera src/api/schema.d.ts a partir do backend
pnpm dev         # sobe em http://localhost:5173
```

## Scripts

| Comando          | Descrição                                          |
| ---------------- | -------------------------------------------------- |
| `pnpm dev`       | Servidor de desenvolvimento                        |
| `pnpm build`     | Build de produção para `dist/`                     |
| `pnpm preview`   | Preview do build                                   |
| `pnpm typecheck` | Verificação de tipos                               |
| `pnpm lint`      | ESLint                                             |
| `pnpm format`    | Prettier                                           |
| `pnpm test`      | Testes com Vitest                                  |
| `pnpm gen:types` | Regenera `src/api/schema.d.ts` a partir do OpenAPI |

## Arquitetura

```
src/
├── api/              Cliente HTTP + tipos gerados
│   ├── client.ts     fetch wrapper com baseURL + Problem Details
│   ├── schema.d.ts   GERADO — não editar
│   └── endpoints/    Um arquivo por recurso
├── hooks/            TanStack Query hooks (useListarExecucoes, ...)
├── components/       Componentes reutilizáveis (Button, Table, ...)
├── features/         Blocos específicos de cada feature
├── pages/            Uma por rota
├── lib/              Utils (formatters, helpers, CSV)
├── test/             Setup do Vitest
├── App.tsx
└── main.tsx
```

## Páginas

| Rota                  | Página            | Função                                         |
| --------------------- | ----------------- | ---------------------------------------------- |
| `/`                   | Dashboard         | Contadores + jobs por status + últimas runs    |
| `/execucoes`          | ExecucoesList     | Listagem paginada com filtros                  |
| `/execucoes/nova`     | ExecucoesNova     | Form de criação (BBOX, baseline P95)           |
| `/execucoes/:id`      | ExecucaoDetalhe   | Detalhe com polling e cancelamento             |
| `/jobs`               | JobsList          | Jobs com filtros + retry                       |
| `/calculos/pontos`    | CalculosPontos    | Cálculo por pontos sync/async                  |
| `/resultados`         | Resultados        | Consulta com filtros URL-persistidos + charts  |
| `/fornecedores`       | Fornecedores      | CRUD + import CSV/XLSX                         |
| `/geocodificacao`     | Geocodificacao    | CIDADE/UF → IBGE + lat/lon                     |
| `/cobertura`          | Cobertura         | Fornecedores com/sem cobertura climática       |
| `/admin`              | Admin             | Contadores + refresh catálogo IBGE             |

Regras:

1. Páginas nunca chamam `fetch` direto — sempre via hook em `hooks/`.
2. Hooks nunca retornam JSX.
3. `schema.d.ts` é a fonte da verdade para contratos do backend.
4. Toda query/mutation trata loading, error e empty state explicitamente.
5. Sem `any`.

## Variáveis de ambiente

| Variável            | Descrição           | Default                 |
| ------------------- | ------------------- | ----------------------- |
| `VITE_API_BASE_URL` | URL base do backend | `http://localhost:8000` |

## Gerando tipos

O backend expõe `/openapi.json`. O script `pnpm gen:types` consome esse
endpoint e escreve `src/api/schema.d.ts`. Rode sempre que o backend mudar
contratos.
