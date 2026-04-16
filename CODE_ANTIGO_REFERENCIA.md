# Código antigo — referência

A pasta `legacy/` contém os scripts originais (`cordex_pr_freq_intensity.py`,
`gera_base_fornecedores.py`, `gera_pontos_fornecedores.py`,
`locais_faltantes_fornecedores.ipynb`) e a documentação em docx. Esse código
serve apenas como **referência** durante a refatoração incremental descrita em
`docs/plano-refatoracao.md`.

Regras até a conclusão do Slice 12:

- **Não modifique** arquivos dentro de `legacy/`. Qualquer correção ou melhoria
  deve acontecer na nova arquitetura (`src/climate_risk/`).
- Scripts legados ainda são executados pelo pipeline de baseline sintética
  (`scripts/gerar_baseline_sintetica.py`) para congelar o comportamento atual.
- A pasta inteira será removida no **Slice 12** ("Polimento e descontinuação"),
  quando todos os casos de uso estiverem portados e validados.
