# ADR-012 — Interface única em `/estudo/`, frontend React em `/app/` desativado

**Status:** Aceito.
**Data:** 2026-04-27.

## Contexto

Até a Slice 19, o backend montava duas interfaces web:

- `/app/` — frontend React/Vite/TypeScript completo
  (`frontend/dist/`), com dashboard, CRUD de fornecedores,
  geocodificação, cobertura, múltiplas execuções, gráficos. Requer build
  com Node + pnpm.
- `/estudo/` — página HTML/CSS/JS puro focada exclusivamente no pipeline
  de estresse hídrico. Funciona sem build.

Na prática, a única interface usada pela pesquisadora que pediu o
pipeline é `/estudo/`. O React em `/app/` ficou abandonado: cobria
funcionalidades que foram cortadas do escopo (cobertura, fornecedores) ou
que nunca foram polidas até o ponto de uso real. Manter as duas em
paralelo cobra três custos:

- **Confusão**: usuários não sabem qual interface usar.
- **Manutenção**: mudanças em `/api/*` exigem atualizar tipos e
  componentes do React mesmo sem ninguém usar.
- **Bootstrap**: novos colaboradores precisam instalar Node + pnpm + rodar
  `pnpm build` antes de ver `/app/` funcionando.

A Slice 20 vai reformar `/estudo/` em três sub-slices (browser de pastas,
export de resultados, redesenho visual). Antes de fazer essa reforma, a
sub-slice 20.1 fecha a outra ponta: desativa `/app/` para reduzir o
escopo de "interfaces a manter" a uma só.

## Decisão

Desativar o mount de `/app/` no FastAPI. Especificamente:

- Remover de `interfaces/app.py` o bloco que monta
  `frontend/dist/` em `/app/` e o catch-all que servia 503 quando o build
  não existia.
- Substituir por um comentário explicativo identificando exatamente quais
  linhas precisam ser descomentadas para reativar a feature.
- **Manter** a pasta `frontend/` intocada — o código React continua
  versionado e funcional, só não é servido pelo backend.
- Remover do `index.html` da página `/estudo/` o link "Interface
  completa" que apontava para `/app/`.
- `GET /app/` e `/app/*` passam a retornar **404** (não mais 503), porque
  o backend não tem mais nada montado nessa rota.
- Nenhum endpoint `/api/*` é afetado. O contrato HTTP da API permanece
  idêntico — toda integração programática continua funcionando.

## Alternativas descartadas

### Apagar a pasta `frontend/`

Eliminaria de vez o custo cognitivo de manter dois mundos, mas perde a
opção de reativar caso uma funcionalidade (ex.: dashboard de gestão)
volte a fazer sentido. O custo de manter o código no repositório é
baixo: ele não é executado, não é importado pelo Python, não roda em
testes. Versão Git é o lugar certo para guardar código pausado mas
potencialmente reutilizável.

### Manter `/app/` mas redirecionar para `/estudo/`

Quebra qualquer URL salva pelos usuários (favoritos, bookmarks, links em
documentos). Como o uso real é zero, essa "compatibilidade" não tem
beneficiário concreto — é peso morto.

### Reaproveitar `/app/` para a nova interface da Slice 20

A nova interface é HTML/CSS/JS puro, igual a `/estudo/`. Reusar o caminho
`/app/` (mesmo trocando o conteúdo) confundiria usuários antigos
acostumados ao React e quebraria a consistência semântica: `/app/` carrega
expectativa de "a aplicação completa", e a Slice 20 é o oposto disso —
foco estreito em estresse hídrico.

## Consequências

**Positivas:**

- Bootstrap mais simples: novos colaboradores só precisam de Python + uv
  para ter a interface principal funcionando.
- Espaço mental livre para a reforma da Slice 20 — uma interface só, um
  caminho único.
- Menos código para manter em sincronia com mudanças do contrato
  `/api/*`.

**Negativas:**

- Quem usava `/app/` (improvável, mas possível) perde o acesso até
  reativar manualmente. A reativação é um diff de 3 linhas no
  `app.py`, então o custo é baixo.
- Se a decisão for revertida, o mount precisa ser religado, mas o build
  do React provavelmente vai precisar de manutenção para acompanhar
  versões de dependências. Essa dívida cresce com o tempo.

## Plano de revisão

Reavaliar quando:

- Surgir caso de uso para um painel administrativo separado da pesquisa
  (gestão de execuções históricas, comparação cruzada, relatórios).
  Nesse cenário, provavelmente reabrir `/app/` (ou criar `/admin/`) com
  scope explícito, em vez de tentar reaproveitar o React legado.
- A pesquisa se expandir para múltiplos pipelines com necessidades
  divergentes — talvez justifique uma interface por pipeline.
