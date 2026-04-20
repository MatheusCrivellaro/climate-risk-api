# Climate Risk UI

Frontend web do **Climate Risk API**. Vive neste subdiretório do repositório
do backend (monorepo simples). Instruções de uso, setup e integração com o
backend estão no [`README.md` da raiz](../README.md#frontend).

Scripts principais (dentro de `frontend/`):

| Comando          | Descrição                                             |
| ---------------- | ----------------------------------------------------- |
| `pnpm dev`       | Servidor de desenvolvimento (`http://localhost:5173`) |
| `pnpm build`     | Build de produção para `dist/`                        |
| `pnpm preview`   | Preview do build                                      |
| `pnpm typecheck` | Verificação de tipos                                  |
| `pnpm lint`      | ESLint                                                |
| `pnpm test`      | Testes com Vitest                                     |
| `pnpm gen:types` | Regenera `src/api/schema.d.ts` a partir do OpenAPI    |
