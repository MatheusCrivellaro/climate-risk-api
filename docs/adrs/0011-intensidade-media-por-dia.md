# ADR-011 — Intensidade de estresse hídrico vira média por dia (mm/dia) em vez de soma anual

**Status:** Aceito.
**Data:** 2026-04-27.

## Contexto

A Slice 13 introduziu o índice anual `intensidade_estresse` (depois
persistido como coluna `intensidade_mm`): **soma** do déficit hídrico
(`evap - pr`) restrita aos dias secos quentes do ano, em mm.

Após uso prático com a pesquisadora que solicitou o pipeline, a definição
mostrou-se inadequada como métrica isolada: ela mistura **frequência** e
**intensidade** num único número.

Exemplo concreto: um município A com 100 dias secos quentes leves
(déficit médio 1 mm/dia) tem `intensidade_mm = 100`; um município B com
20 dias secos quentes severos (déficit médio 8 mm/dia) tem
`intensidade_mm = 160`. A leitura ingênua é "B é mais intenso", mas A
acumulou mais déficit total — e sem olhar para `frequencia` separadamente
não há como saber qual estresse é "mais severo por dia".

A pesquisadora pediu que `intensidade` passasse a representar a
**severidade média de cada dia em estresse**, isolada da frequência.

## Decisão

Trocar a definição de `intensidade` para a **média** do déficit hídrico
nos dias secos quentes do ano:

```python
intensidade_mm_dia = soma(deficit nos dias secos quentes) / frequencia_dias_secos_quentes
```

- **Unidade**: `mm/dia` (a coluna passa a se chamar `intensidade_mm_dia`).
- **Divisão por zero**: quando `frequencia == 0`, definimos
  `intensidade_mm_dia = 0.0` (interpretação: "não houve estresse"). Não
  retornamos `NaN` para manter a coluna sempre numérica e evitar poluir
  consultas downstream com nulos.
- **Dados existentes**: descartados. A única execução em produção era
  de teste e será refeita. A migração Alembic (`5c2d8a17b9f4`) faz
  `DELETE FROM resultado_estresse_hidrico` antes do `ALTER TABLE ...
  RENAME COLUMN`. **Não há recálculo nem migração de dados**.
- **Schema**: rename direto `intensidade_mm` → `intensidade_mm_dia`. Não
  manter ambas em paralelo — coluna nova, sem fallback.

## Alternativas descartadas

### Manter ambas (`intensidade_mm` e `intensidade_mm_dia`)

Dobraria a coluna na tabela e adicionaria ambiguidade no consumo (qual
usar? por quê?). Sem caso de uso real para a soma anual, não vale a pena
o custo.

### Recalcular os dados existentes em migração

A única execução existente é de teste e será refeita pela pesquisadora.
Recalcular significaria reler os NetCDFs, refazer o agregador
geoespacial e regravar — toda a complexidade do worker dentro de uma
migration, sem ganho prático.

### Adicionar coluna calculada (`intensidade_mm_dia` como `intensidade_mm
/ frequencia`) deixando os dados antigos preservados

SQLite não tem suporte robusto a colunas geradas com triggers, e a
expressão calculada não cobre o caso `frequencia == 0` (divisão por
zero). Persistir o valor já dividido é mais simples, mais portável
(PostgreSQL futuro) e respeita a convenção `0.0` quando frequência é zero.

### Renomear apenas no Pydantic (mantendo coluna SQL como `intensidade_mm`)

Quebraria a regra de manter naming consistente entre camadas, e o nome
da coluna no banco apareceria em logs/queries diretas como mentira sobre
a unidade. Renomeamos em todas as camadas.

## Consequências

**Positivas:**

- A métrica passa a ter interpretação direta: "quão severo é, em média,
  cada dia de estresse hídrico no ano".
- Frequência e intensidade ficam ortogonais — análise comparativa entre
  municípios fica mais clara.
- Unidade (`mm/dia`) bate com o domínio (déficit é tipicamente expresso
  em mm/dia em hidrologia operacional).

**Negativas:**

- Quebra de contrato HTTP: o campo JSON passou de `intensidade_mm` para
  `intensidade_mm_dia`. Clientes terceiros (se houvesse) precisariam
  atualizar. Como não há clientes em produção fora do nosso frontend,
  o custo é apenas atualizar `frontend/` e `/estudo/` neste mesmo PR.
- Perda dos dados antigos. Aceito porque eram dados de teste.
- A definição de "intensidade total acumulada no ano" (que a soma
  representava) deixa de estar disponível como atalho. Quem precisar
  pode multiplicar `intensidade_mm_dia × frequencia_dias_secos_quentes`
  no SQL de consulta — já que ambas as colunas continuam disponíveis.

## Plano de revisão

Reavaliar quando:

- Aparecer caso de uso para a soma anual original (improvável — a
  pesquisadora pediu explicitamente a troca).
- Surgir uma terceira métrica que combine frequência e intensidade
  (ex.: índice composto). Seria coluna nova; a definição atual de cada
  uma fica intocada.
