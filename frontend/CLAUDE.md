# CLAUDE.md — Estado do projeto

Documento vivo para contextualizar novas sessões do Claude Code caso a
implementação em slices tenha que ser retomada em outra janela.

## Contexto

Frontend web para o **Climate Risk API**. Implementação feita em slices
conforme prompt inicial. Cada slice termina em commit limpo e passa em
`pnpm typecheck && pnpm lint && pnpm test && pnpm build`.

## Stack

React 18 + Vite 5 + TS strict, Tailwind 3, TanStack Query v5, React Router
v6, Recharts, openapi-typescript. Ver `package.json` para pinos exatos.

## Estado dos slices

- [x] **Slice 0 — Setup** (Vite + Tailwind + tooling + CLAUDE.md + README)
- [x] Slice 1 — API client + tipos gerados
- [x] Slice 2 — Componentes base + layout
- [x] Slice 3 — Dashboard
- [x] Slice 4 — Execuções
- [x] Slice 5 — Jobs + cálculos por pontos
- [x] Slice 6 — Resultados + agregações
- [x] Slice 7 — Fornecedores
- [x] Slice 8 — Geocodificação + cobertura
- [x] Slice 9 — Polimento + testes + docs

## Decisões tomadas

- O frontend vive em `frontend/` dentro do repo do backend (monorepo simples).
- `src/api/schema.d.ts` começa como stub vazio; a geração real depende do
  backend estar rodando. `pnpm gen:types` falha com mensagem explicativa.
- ESLint em modo legacy (`.eslintrc.cjs`) em vez de flat config para
  compatibilidade com a versão 8 que ainda é a mais estável.
- Paleta `primary` definida em `tailwind.config.js`. Fontes via Google Fonts
  no `index.css`.
- `strict: true`, `noUncheckedIndexedAccess: true`, `noImplicitOverride: true`
  em `tsconfig.app.json`.
- `frontend/.gitignore` anula o `lib/` do `.gitignore` raiz (que é padrão
  Python), senão `frontend/src/lib/` não é versionado.
- Testes Vitest + Testing Library focados em utilitários puros
  (`parseCidadeUfText`, `parsePontosCSV`, `toCSV`) + smoke de
  `Button` e `Sidebar`. Páginas com efeitos de rede ficam fora da
  cobertura automatizada por enquanto.

## Como retomar

1. `cd frontend && pnpm install`
2. Ler este arquivo e os últimos commits (`git log --oneline`).
3. Continuar a partir do primeiro slice não marcado acima.
4. Ao fim de cada slice: `pnpm typecheck && pnpm lint && pnpm test && pnpm build`
   deve passar limpo.
5. Commit com mensagem no estilo Conventional Commits.
6. Atualizar a checklist de slices acima.

## Convenções

- Português no domínio (`useListarExecucoes`, `criarFornecedor`), inglês em
  constructs técnicos (`QueryClient`, `useMemo`).
- PascalCase para componentes, um por arquivo.
- Imports agrupados: libs externas → `@/...` → relativo.
- Sem `any`, sem `@ts-ignore`, sem `console.log` em produção.
- Commit: Conventional Commits.
