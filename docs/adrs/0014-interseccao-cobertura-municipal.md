# ADR-014 — Interseção de cobertura municipal entre grades de variáveis

**Status:** Aceito.
**Data:** 2026-04-28.

## Contexto

A Slice 21 (ADR-013) introduziu pipeline streaming para estresse hídrico:
o handler consumia 3 iteradores paralelos (`pr`, `tas`, `evspsbl`) via
`AgregadorEspacial.iterar_por_municipio` e pareava as tuplas com `zip`.
A ordem determinística por `municipio_id` garantia o pareamento
desde que **as 3 grades cobrissem o mesmo conjunto de municípios**.

Em produção, com dados reais, esse pressuposto falhou:

```
RuntimeError: Inconsistência de iteração: pr=1100031, tas=1100031,
evap=1100049. Verificar determinismo da ordem em
AgregadorEspacial.iterar_por_municipio.
```

Investigação mostrou que **não** há falha de determinismo. As variáveis
`pr` e `tas` vêm do mesmo modelo climático (5 arquivos cada), enquanto
`evspsbl` vem de outra fonte (15 arquivos, grade ligeiramente diferente).
Quando o agregador faz a interseção espacial célula→município em cada
grade separadamente, os conjuntos resultantes diferem — o município
`1100031` aparece em `pr`/`tas` mas não em `evspsbl`, e o `1100049`
aparece nas 3 mas em posições diferentes da iteração ordenada.

A Slice 21 assumiu paridade entre as 3 grades, e isso não vale para
modelos climáticos reais — coberturas geográficas podem divergir
especialmente em bordas costeiras e fronteiras.

## Decisão

**Pipeline calcula a interseção dos 3 conjuntos de municípios mapeados e
processa apenas essa interseção, logando warning estruturado com as
divergências.**

Mudanças concretas:

1. **Porta `AgregadorEspacial`** ganha 2 métodos novos:
   - `municipios_mapeados(dados) -> set[int]` — retorna apenas o conjunto
     de IDs mapeados na grade, sem materializar séries (operação leve).
   - `serie_de_municipio(dados, municipio_id) -> (datas, serie)` —
     retorna a série diária de **um** município específico. Levanta
     `KeyError` se o município não está mapeado nesta grade.
2. **Adapter `AgregadorMunicipiosGeopandas`** implementa os dois com
   cache em memória do mapeamento célula→município por hash da grade.
   Chamadas consecutivas para a mesma grade reusam o mapa sem ler o
   parquet do disco.
3. **Handler `_processar_streaming`** substitui `zip(iter_pr, iter_tas,
   iter_evap)` por iteração explícita pela interseção. Para cada
   município, busca a série nas 3 grades individualmente. Divergências
   são logadas como warning estruturado mostrando contagens e amostras
   por categoria (município só em `pr`, só em `tas`, em `pr ∩ tas` mas
   não em `evap`, etc.).
4. **`iterar_por_municipio` é mantido** — continua sendo usado pelo
   método legacy `agregar_por_municipio` e pelos testes da Slice 21.
   Docstring atualizada explicitando que não deve ser pareado em
   paralelo para variáveis distintas.

## Alternativas descartadas

### A — Pular silenciosamente

Filtrar os iteradores para a interseção sem qualquer log. Implementação
mais simples, mas perda de dados invisível ao usuário é pior que
perda explícita: bordas costeiras podem perder dezenas a centenas de
municípios, e o usuário precisa decidir se a divergência é aceitável
para a finalidade (relatório, modelo agregado, comparação espacial
detalhada).

### C — Falhar duro (manter `RuntimeError`)

Continuar levantando erro quando as grades divergem. Robustez máxima
sob a tese "se os dados não batem, o usuário precisa saber e decidir o
que fazer". Mas é exageradamente intrusivo para divergências pequenas e
naturais (1 município numa borda costeira) e bloqueia o pipeline mesmo
quando o resultado parcial é perfeitamente útil. Reproduzia exatamente
o bug em produção.

### B (escolhida) — Interseção + warning estruturado

Balanço entre robustez e transparência:

- O pipeline funciona em dados reais sem intervenção manual.
- Cada execução com divergência produz log de warning com counts e
  amostras suficientes para o usuário entender o que perdeu.
- Logs estruturados (`extra=`) permitem agregação posterior em sistemas
  de observabilidade.
- O log captura todas as 6 categorias possíveis de divergência (só `pr`,
  só `tas`, só `evap`, `pr ∩ tas` sem `evap`, `pr ∩ evap` sem `tas`,
  `tas ∩ evap` sem `pr`), úteis para diagnosticar a fonte da
  divergência.

## Consequências

**Positivas:**

- Pipeline tolera as divergências naturais entre modelos climáticos sem
  precisar de pré-processamento manual de cobertura.
- A interseção é determinística (intersecção de conjuntos é
  comutativa/associativa), idempotente e reproduzível.
- O método novo `serie_de_municipio` desacopla a iteração do agregador
  da ordem natural da grade — útil para outros casos de uso futuros
  (ex.: processar uma cidade específica sem iterar todas as outras).

**Negativas:**

- O resultado pode ter menos municípios que o conjunto total presente em
  qualquer das 3 grades. Para usuários que esperam paridade
  município ↔ shapefile, isso pode surpreender.
- Bordas costeiras tendem a ser as mais afetadas (linha de costa varia
  de modelo para modelo). Análises focadas em municípios costeiros
  precisam verificar o warning antes de interpretar resultados.
- O log de warning é por execução: se múltiplas execuções rodam
  simultaneamente com mesmo padrão de divergência, o ruído cresce
  linearmente. Mitigação: log estruturado com `execucao_id` permite
  filtrar por execução.

## Plano de revisão

Reavaliar quando:

- Aparecer pressão para preencher municípios divergentes via
  interpolação ou substituição (ex.: usar média regional para
  municípios cobertos por apenas 2 das 3 grades). Hoje a posição é "se
  não há dado nas 3, não há resultado", e isso é simples e auditável.
- Os logs de warning revelarem que a divergência é sistemática (e.g.,
  >5% dos municípios pulados em todo dataset). Pode justificar
  pré-processamento upstream para alinhar grades antes do pipeline.
- For adicionada uma 4ª variável ao pipeline. A interseção continua
  funcionando, mas o log de divergência precisa de mais 6 categorias
  (cresce com `2^N - N - 1`); revisar se a estrutura ainda escala.
