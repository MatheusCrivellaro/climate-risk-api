# ADR-007 — Detecção de Unidade de Precipitação: Estratégia Atual e Plano de Migração

**Status:** Aceito com ressalva.
**Data:** 2026-04-16.

## Contexto

O código atual converte precipitação para `mm/dia` usando a condição:

```python
if ("kg m-2 s-1" in units) or ("kg m^-2 s^-1" in units) or \
   ("mm s-1" in units) or ("mm/s" in units) or vmax < 5.0:
    da = da * 86400.0
```

A cláusula `vmax < 5.0` é uma heurística não documentada: se o máximo do `DataArray` for menor que 5, assume-se que está em fluxo. Isso pode falhar em células áridas ou períodos curtos, onde um dado legítimo em `mm/day` tem `vmax < 5` e seria convertido indevidamente (multiplicado por 86400).

### Análise de risco

| Cenário | Comportamento |
|---|---|
| Dado em `kg m-2 s-1` com qualquer `vmax` | ✅ Convertido corretamente |
| Dado em `mm/day` com célula seca (`vmax < 5`) | ⚠️ **Convertido indevidamente** (~100× irreal) |
| Dado em `mm/day` com ao menos um dia `>= 5 mm` | ✅ Mantido |

## Decisão (MVP)

Preservar o comportamento atual bit-a-bit. A função é portada para a camada `domain` com o mesmo algoritmo, permitindo validação de paridade com a baseline de regressão.

## Decisão (pós-MVP)

Substituir por detecção baseada exclusivamente em metadados, com *fail-fast* quando metadados forem ambíguos:

1. Se `units` é reconhecível como fluxo → converter.
2. Se `units` é reconhecível como acumulado diário (`mm`, `mm/day`, `kg m-2 day-1`) → manter.
3. Caso contrário → erro explícito, obrigando configuração manual via parâmetro.

A migração requer:
- Inventário das unidades realmente encontradas nos arquivos da empresa.
- Decisão sobre dado sem metadados (rejeitar? aceitar com override?).
- Atualização de testes.

## Consequências

**Positivas:**
- No MVP: paridade numérica com o sistema atual, evita regressões.
- Pós-MVP: elimina classe de bugs silenciosos; torna o sistema auditável.

**Negativas:**
- Durante o MVP, a heurística continua produzindo resultados potencialmente incorretos em casos de canto. Documentado como risco aceito (R-01).
- A decisão pós-MVP exige coordenação com o time de dados.
