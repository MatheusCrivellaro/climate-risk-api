# Dívida Técnica

Registro das dívidas técnicas contraídas e seu estado atual. Uma dívida
"aceita" é mantida deliberadamente; uma "resolvida" foi revertida ou
substituída.

| ID | Título | Introduzida em | Estado | Resolvida em |
|---|---|---|---|---|
| DT-001 | `CalcularIndicesPorPontos` com flag `persistir` | Slice 7 | Resolvida | Slice 12 |
| DT-002 | Nome do módulo `geocodificacao` | Slice 8 | Aceita | — |
| DT-003 | `Municipio.id` como `int` | Slice 2 | Aceita | — |

## DT-001 — `CalcularIndicesPorPontos` com flag `persistir`

**Status:** Resolvida (Slice 12).

**Contexto (Slice 7).** Ao implementar o fluxo assíncrono de UC-03
(worker + fila), reaproveitei o caso de uso síncrono do Slice 4
adicionando um parâmetro `persistir: bool` em `ParametrosCalculo` e
injetando `RepositorioExecucoes`/`RepositorioResultados` no construtor.
O worker chamava o caso de uso com `persistir=False` e depois persistia
ele mesmo; a rota síncrona passava `persistir=payload.persistir`.

**Por que foi ruim.** Um caso de uso puro ficou com duas personalidades
— calcula e às vezes persiste —, violando SRP e acoplando camadas que
deveriam ficar separadas. O construtor exigia repositórios mesmo quando
o caller não precisava deles.

**Resolução (Slice 12).**
- Removidos `persistir` e os campos de repositório do caso de uso síncrono.
- `ResultadoCalculo` deixou de expor `execucao_id`.
- `ProcessarPontosLote` (worker) passou a instanciar
  `CalcularIndicesPorPontos(leitor_netcdf=...)` sem repositórios e
  continua responsável pela persistência, como já era.
- O schema Pydantic `CalculoPorPontosRequest` perdeu o campo
  `persistir`; a rota `POST /calculos/pontos` retorna apenas os
  resultados calculados. Quem precisa de persistência usa o fluxo
  assíncrono (`202`) ou `POST /execucoes`.
- Testes obsoletos que validavam persistência via endpoint síncrono
  foram removidos — a cobertura da persistência vive em
  `ProcessarPontosLote` e no fluxo CORDEX.

## DT-002 — Nome do módulo `geocodificacao`

**Status:** Aceita.

**Contexto (Slice 8).** O módulo foi nomeado
`application/geocodificacao/` em português, seguindo a convenção do
restante do projeto (`application/execucoes`, `application/fornecedores`
etc.). Ponto de atrito menor: a palavra técnica "geocoding" é mais
comum em literatura internacional.

**Decisão.** Manter o nome em português. Renomear exigiria cascata em
imports, `dependencias.py`, rotas e documentação sem benefício funcional.
A consistência linguística do projeto tem mais valor do que o alinhamento
com um termo técnico isolado.

## DT-003 — `Municipio.id` como `int`

**Status:** Aceita.

**Contexto (Slice 2).** A entidade `Municipio` usa `id: int`
(código IBGE de 7 dígitos) em vez do padrão ULID-string (`mun_...`)
adotado nas outras entidades.

**Decisão.** Manter `int`. O código IBGE já é um identificador
estável, universalmente aceito e referenciado em fontes externas
(shapefiles, CSVs, API do IBGE). Trocar para ULID exigiria cascata de
migrações em FKs (`ResultadoIndice.municipio_id`, cobertura de
fornecedores) e quebraria a semântica externa do identificador sem
benefício técnico real.
