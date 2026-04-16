# Análise Exploratória Inicial da Base de Código

## 1. Leitura da Documentação (`code_doc.docx`)

### 1.1 Propósito declarado
A documentação descreve um único script — `cordex_pr_freq_intensity.py` — com o objetivo de processar arquivos NetCDF diários do projeto CORDEX, recortar o território brasileiro via *bounding box*, calcular índices anuais de **frequência**, **intensidade** e **extremos de precipitação** por ponto de grade, e exportar o resultado consolidado em CSV.

### 1.2 Escopo funcional declarado
- Leitura de múltiplos `.nc` com diferentes *engines* (`netcdf4`, `h5netcdf`, `scipy`).
- Inferência do cenário climático (`rcp*` / `ssp*`) a partir do nome do arquivo ou atributos internos.
- Conversão de unidades (`kg m-2 s-1` → `mm/dia`).
- Recorte espacial por BBOX; normalização de longitudes 0–360 → -180–180.
- Cálculo de índices anuais por célula: `wet_days`, `sdii`, `rx1day`, `rx5day`, `r20mm`, `r50mm`, `r95ptot_mm`, `r95ptot_frac`.
- Geocodificação opcional de UF/município via shapefiles (IBGE).
- Emissão de CSV principal + CSV auxiliar (`_how_calculated.csv`).
- Seção de adaptação futura para a variável **evaporação**.

### 1.3 Tecnologias mencionadas
`numpy`, `pandas`, `xarray`, `netcdf4` / `h5netcdf`, `geopandas`, `shapely`.

### 1.4 Regras de negócio explícitas
| Regra | Origem |
|---|---|
| `wet_days` = nº de dias com `pr >= T` (default T = 20 mm) | §5.2 e §6.4 |
| `sdii` = média de `pr` **apenas** nos dias com `pr >= T` | §5.2 |
| `rx5day` calculado sobre janela **móvel** de 5 dias | §5.2 |
| P95 do baseline usa apenas dias com `pr >= p95_wet_thr` (default 1.0 mm, padrão ETCCDI) | §6.5 |
| Conversão de unidade só é aplicada quando as unidades indicam fluxo | §6.3 |
| Um dos argumentos `--input-dir` ou `--glob` é obrigatório | §4 |

### 1.5 Lacunas e contradições na documentação

| # | Problema |
|---|---|
| 1 | A documentação cobre apenas 1 de 4 scripts do projeto |
| 2 | A conversão de unidade tem heurística `vmax < 5.0` não documentada |
| 3 | BBOX sem explicação da convenção de sinais |
| 4 | Seção sobre evaporação não especifica a variável CORDEX a ser usada |
| 5 | Sem menção a testes, validação, logs estruturados |
| 6 | Recomenda UTF-8 mas o pipeline usa `latin-1` em um dos CSVs |

## 2. Inventário dos Arquivos

| Arquivo | LOC | Papel | Observações |
|---|---|---|---|
| `cordex_pr_freq_intensity.py` | ~420 | Pipeline principal (grade completa via BBOX) | Único coberto pela doc |
| `gera_pontos_fornecedores.py` | ~350 | Pipeline CORDEX para pontos exatos | Duplicação significativa do anterior |
| `gera_base_fornecedores.py` | ~240 | Geocodificação via IBGE + match fuzzy | `RAW_LISTA` hardcoded |
| `locais_faltantes_fornecedores.ipynb` | ~30 células | Reconciliação fornecedores × grade | Exploratório; não reprodutível isolado |
| `teste.py` | 1 | Apenas `print("hello world")` | Código morto |

## 3. Mapeamento de Dependências

### 3.1 Dependências externas
`numpy`, `pandas`, `xarray`, `netcdf4` / `h5netcdf`, `geopandas`, `shapely`, `requests`, `urllib3`, `unidecode`, `rapidfuzz`, `openpyxl`, `cftime` (implícito).

**Sem `requirements.txt` ou `pyproject.toml`** — versões não detectáveis.

### 3.2 Dependências internas
Os arquivos Python **não se importam entre si**. Acoplamento é exclusivamente via filesystem (CSVs intermediários).

### 3.3 Pontos de entrada
| Arquivo | Entry point |
|---|---|
| `cordex_pr_freq_intensity.py` | CLI via `argparse` |
| `gera_pontos_fornecedores.py` | CLI via `argparse` |
| `gera_base_fornecedores.py` | Script com `RAW_LISTA` hardcoded |
| `locais_faltantes_fornecedores.ipynb` | Notebook interativo |
| `teste.py` | Trivial |

## 4. Arquitetura Implícita

**Padrão atual:** conjunto de scripts procedurais independentes, acoplados via filesystem. Não há camadas, nem abstrações sobre I/O, nem módulo compartilhado — apesar de existirem ~15 funções duplicadas entre os dois scripts CORDEX.

## 5. Primeiros Sinais de Dívida Técnica

### Alta prioridade
- **DT-01** Duplicação massiva entre os dois scripts CORDEX.
- **DT-02** Lista de entrada hardcoded em `gera_base_fornecedores.py`.
- **DT-03** Ausência total de testes automatizados.
- **DT-04** Ausência de `requirements.txt` / `pyproject.toml`.
- **DT-05** Heurística `vmax < 5.0` na conversão de unidade não documentada.
- **DT-06** Mistura de responsabilidades em `write_rows` e `process_file`.

### Média prioridade
- **DT-07** `teste.py` é código morto.
- **DT-08** Cabeçalhos/docstrings duplicados em `gera_base_fornecedores.py`.
- **DT-09** Encoding inconsistente nos CSVs.
- **DT-10** Tipagem parcial.
- **DT-11** Tratamento de erro via `except Exception` genérico.
- **DT-12** `log()` ad-hoc reimplementado por arquivo.
- **DT-13** Notebook não reprodutível isoladamente.
- **DT-14** Nomenclatura mista PT/EN.

### Baixa prioridade
- **DT-15** Imports não utilizados em `gera_base_fornecedores.py`.
- **DT-16** Caracteres especiais em docstrings.
- **DT-17** Linhas em branco / comentários vazios.

## 6. Divergências entre Código e Documentação

| # | Doc afirma | Código faz | Severidade |
|---|---|---|---|
| D-01 | Converte quando metadados indicam fluxo | Converte também via `vmax < 5.0` | Alta |
| D-02 | Descreve 1 pipeline | Existem 2 pipelines + geocodificação + notebook | Alta |
| D-03 | Recomenda UTF-8 | `gera_base_fornecedores.py` usa `latin-1` | Média |
| D-04 | Menciona apenas `pr` | Existe fluxo paralelo de geocodificação | Alta |
| D-05 | Define `wet_days` genérico | Default 20 mm é limiar alto, não-ETCCDI | Média |
| D-06 | Cita P95 do baseline | Não diferencia `p95_wet_thr` de `freq_thr_mm` | Média |

## 7. Resumo Executivo

O repositório contém um **sistema não declarado em sua documentação**: um pipeline de avaliação de risco climático associado à localização de fornecedores, composto por quatro estágios acoplados via filesystem. A documentação `.docx` descreve apenas um dos estágios e o faz de forma parcialmente inconsistente com a implementação. O principal problema arquitetural identificado é a **duplicação de ~15 funções utilitárias** entre os dois scripts CORDEX, sem qualquer módulo compartilhado. A principal lacuna de qualidade é a **ausência completa de testes e de especificação de ambiente**.
