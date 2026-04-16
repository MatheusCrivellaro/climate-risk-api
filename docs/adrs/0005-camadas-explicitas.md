# ADR-005 — Arquitetura em Camadas Explícitas

**Status:** Aceito.
**Data:** 2026-04-16.

## Contexto

O código atual é procedural e monolítico. O grau de estruturação escolhido é "B — camadas explícitas", ficando abaixo de hexagonal pura mas acima de separação puramente pragmática. É necessário definir as camadas e suas regras de dependência.

## Decisão

Adotar quatro camadas com direção de dependência estrita (setas apontam para dentro):

```
interfaces  →  application  →  domain
                    ↓
             infrastructure  →  (externo: filesystem, HTTP, banco)
```

### Responsabilidade de cada camada

- **`domain`**: entidades e regras de negócio puras. Calculadoras de índices, conversores de unidade, contratos de repositório (interfaces). Sem I/O, sem frameworks web, sem ORM, sem HTTP client.
- **`application`**: casos de uso (orquestradores). Cada caso de uso mapeia um fluxo de negócio. Depende de `domain` e de interfaces declaradas em `domain`. Sem conhecimento de FastAPI, SQLAlchemy ou detalhes de persistência.
- **`infrastructure`**: implementações concretas das interfaces. Adaptadores NetCDF, cliente IBGE, leitor shapefile, repositórios SQLAlchemy, fila SQLite. Pode depender de qualquer pacote externo.
- **`interfaces`**: ponto de entrada HTTP (FastAPI). Controllers, schemas Pydantic, configuração de rotas. Traduz HTTP → casos de uso → HTTP.

### Regras de dependência

- `domain` não importa de nenhuma das outras camadas.
- `application` só depende de `domain`.
- `infrastructure` implementa interfaces de `domain` e pode importar de qualquer lugar do projeto.
- `interfaces` compõe: injeta implementações de `infrastructure` nos casos de uso de `application`.

### Tipagem
- `mypy --strict` em `domain` e `application`.
- `mypy` padrão em `infrastructure` e `interfaces`.

## Consequências

**Positivas:**
- Núcleo numérico testável sem banco, sem FastAPI, sem arquivos reais.
- Troca de SQLite → PostgreSQL fica contida em `infrastructure`.
- Troca de fila SQLite → ARQ fica contida em `infrastructure`.
- Troca de FastAPI → outro framework fica contida em `interfaces`.
- Extensão a evaporação exige mudanças principalmente em `domain` e `infrastructure`, sem tocar `interfaces`.

**Negativas:**
- Mais arquivos e módulos do que um script único.
- Injeção de dependência precisa ser disciplinada — risco de "vazar" tipos concretos para `application`.
- Não é hexagonal pura: não há separação estrita entre ports e adapters, o que é aceito.
