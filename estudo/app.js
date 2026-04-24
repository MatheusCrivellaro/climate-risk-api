// Página /estudo/ — JavaScript vanilla.
// Consome os endpoints /api/execucoes/estresse-hidrico e
// /api/resultados/estresse-hidrico já existentes.

'use strict';

const API_BASE = '/api';
const POLLING_INTERVAL_MS = 3000;
const PAGINA_TAMANHO = 50;
const STATUSES_FINAIS = new Set(['completed', 'failed', 'canceled']);

const UFS = [
  'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
  'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
  'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO',
];

const state = {
  execucaoAtual: null,
  pollingTimer: null,
  paginaAtual: 0,
  totalResultados: 0,
  filtrosAtivos: {},
  grafico: null,
};

// -------------------------------------------------------------------------
// Helpers genéricos
// -------------------------------------------------------------------------

function qs(id) {
  return document.getElementById(id);
}

function formatarNumero(valor, casas = 2) {
  if (valor === null || valor === undefined || Number.isNaN(valor)) {
    return '—';
  }
  return Number(valor).toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  });
}

function escapeHtml(valor) {
  if (valor === null || valor === undefined) return '';
  return String(valor)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function chamarApi(path, options = {}) {
  const resposta = await fetch(`${API_BASE}${path}`, {
    headers: { Accept: 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const contentType = resposta.headers.get('content-type') || '';
  const corpo = contentType.includes('json') ? await resposta.json() : null;
  if (!resposta.ok) {
    const detalhe =
      (corpo && (corpo.detail || corpo.title)) ||
      `${resposta.status} ${resposta.statusText}`;
    const erro = new Error(detalhe);
    erro.status = resposta.status;
    erro.problem = corpo;
    throw erro;
  }
  return corpo;
}

// -------------------------------------------------------------------------
// Seção 1: criar execução + polling
// -------------------------------------------------------------------------

async function criarExecucao(event) {
  event.preventDefault();
  pararPolling();

  const feedback = qs('feedback-criar');
  feedback.className = 'feedback';
  feedback.textContent = '';

  const form = event.currentTarget;
  const dados = {
    arquivo_pr: form.arquivo_pr.value.trim(),
    arquivo_tas: form.arquivo_tas.value.trim(),
    arquivo_evap: form.arquivo_evap.value.trim(),
    cenario: form.cenario.value,
    parametros: {
      limiar_pr_mm_dia: Number(form.limiar_pr_mm_dia.value),
      limiar_tas_c: Number(form.limiar_tas_c.value),
    },
  };

  if (!dados.arquivo_pr || !dados.arquivo_tas || !dados.arquivo_evap) {
    mostrarFeedbackErro('Informe os três caminhos de arquivo.');
    return;
  }

  const botao = qs('btn-criar');
  botao.disabled = true;
  botao.textContent = 'Criando...';

  try {
    const resposta = await chamarApi('/execucoes/estresse-hidrico', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dados),
    });
    state.execucaoAtual = {
      id: resposta.execucao_id,
      jobId: resposta.job_id,
      status: resposta.status,
    };
    renderizarFeedbackExecucao();
    iniciarPolling(resposta.execucao_id);
  } catch (erro) {
    mostrarFeedbackErro(erro.message || 'Falha ao criar execução.');
  } finally {
    botao.disabled = false;
    botao.textContent = 'Criar execução';
  }
}

function mostrarFeedbackErro(mensagem) {
  const feedback = qs('feedback-criar');
  feedback.className = 'feedback feedback-erro';
  feedback.textContent = mensagem;
}

function renderizarFeedbackExecucao() {
  const feedback = qs('feedback-criar');
  const exec = state.execucaoAtual;
  if (!exec) {
    feedback.className = 'feedback';
    feedback.textContent = '';
    return;
  }
  feedback.className = 'feedback feedback-info';
  feedback.innerHTML = `
    <span>Execução: <code>${escapeHtml(exec.id)}</code></span>
    <span class="status-badge status-${escapeHtml(exec.status)}">${escapeHtml(exec.status)}</span>
    <button type="button" id="btn-ver-resultados" class="btn btn-sucesso">
      Ver resultados
    </button>
  `;
  const botaoVer = qs('btn-ver-resultados');
  if (botaoVer) {
    botaoVer.addEventListener('click', () => irParaResultadosDaExecucao(exec.id));
  }
}

function iniciarPolling(execucaoId) {
  pararPolling();
  state.pollingTimer = setInterval(
    () => atualizarStatusExecucao(execucaoId),
    POLLING_INTERVAL_MS,
  );
}

function pararPolling() {
  if (state.pollingTimer !== null) {
    clearInterval(state.pollingTimer);
    state.pollingTimer = null;
  }
}

async function atualizarStatusExecucao(execucaoId) {
  try {
    const resposta = await chamarApi(
      `/execucoes/${encodeURIComponent(execucaoId)}`,
    );
    if (!state.execucaoAtual || state.execucaoAtual.id !== execucaoId) {
      pararPolling();
      return;
    }
    state.execucaoAtual.status = resposta.status;
    renderizarFeedbackExecucao();
    if (STATUSES_FINAIS.has(resposta.status)) {
      pararPolling();
      if (resposta.status === 'completed') {
        irParaResultadosDaExecucao(execucaoId);
      }
    }
  } catch (erro) {
    pararPolling();
    mostrarFeedbackErro(
      `Falha ao consultar status da execução: ${erro.message || erro}`,
    );
  }
}

function irParaResultadosDaExecucao(execucaoId) {
  qs('filtro-execucao').value = execucaoId;
  qs('filtro-cenario').value = '';
  qs('filtro-ano-min').value = '';
  qs('filtro-ano-max').value = '';
  qs('filtro-uf').value = '';
  qs('resultados-titulo').scrollIntoView({ behavior: 'smooth', block: 'start' });
  buscarResultados();
}

// -------------------------------------------------------------------------
// Seção 2: consultar resultados
// -------------------------------------------------------------------------

function lerFiltrosDoForm() {
  const execucao = qs('filtro-execucao').value.trim();
  const cenario = qs('filtro-cenario').value;
  const anoMin = qs('filtro-ano-min').value.trim();
  const anoMax = qs('filtro-ano-max').value.trim();
  const uf = qs('filtro-uf').value;

  const filtros = {};
  if (execucao) filtros.execucao_id = execucao;
  if (cenario) filtros.cenario = cenario;
  if (anoMin) filtros.ano_min = Number(anoMin);
  if (anoMax) filtros.ano_max = Number(anoMax);
  if (uf) filtros.uf = uf;
  return filtros;
}

function montarQueryString(filtros, extras) {
  const params = new URLSearchParams();
  for (const [chave, valor] of Object.entries(filtros)) {
    if (valor !== undefined && valor !== null && valor !== '') {
      params.set(chave, String(valor));
    }
  }
  for (const [chave, valor] of Object.entries(extras || {})) {
    params.set(chave, String(valor));
  }
  const qs = params.toString();
  return qs ? `?${qs}` : '';
}

async function submeterBusca(event) {
  event.preventDefault();
  state.paginaAtual = 0;
  state.filtrosAtivos = lerFiltrosDoForm();
  await buscarResultados();
}

async function buscarResultados() {
  const contador = qs('contador-resultados');
  contador.textContent = 'Carregando...';
  const tbody = qs('tbody-resultados');
  tbody.innerHTML = '';

  const queryString = montarQueryString(state.filtrosAtivos, {
    limit: PAGINA_TAMANHO,
    offset: state.paginaAtual * PAGINA_TAMANHO,
  });

  try {
    const resposta = await chamarApi(
      `/resultados/estresse-hidrico${queryString}`,
    );
    state.totalResultados = resposta.total;
    renderizarTabela(resposta.items);
    atualizarContador(resposta);
    atualizarPaginacao(resposta);
    renderizarGrafico(resposta.items);
  } catch (erro) {
    contador.textContent = '';
    renderizarErroNaTabela(erro.message || 'Falha ao consultar resultados.');
    destruirGrafico();
  }
}

function renderizarTabela(items) {
  const tbody = qs('tbody-resultados');
  if (!items || items.length === 0) {
    tbody.innerHTML =
      '<tr class="linha-vazia"><td colspan="6">Nenhum resultado para os filtros aplicados.</td></tr>';
    return;
  }
  const linhas = items.map((item) => {
    const municipio = item.nome_municipio
      ? escapeHtml(item.nome_municipio)
      : `<code>${escapeHtml(item.municipio_id)}</code>`;
    return `
      <tr>
        <td>${municipio}</td>
        <td>${escapeHtml(item.uf ?? '—')}</td>
        <td>${escapeHtml(item.cenario)}</td>
        <td>${escapeHtml(item.ano)}</td>
        <td>${escapeHtml(item.frequencia_dias_secos_quentes)}</td>
        <td>${formatarNumero(item.intensidade_mm)}</td>
      </tr>
    `;
  });
  tbody.innerHTML = linhas.join('');
}

function renderizarErroNaTabela(mensagem) {
  const tbody = qs('tbody-resultados');
  tbody.innerHTML = `
    <tr class="linha-vazia">
      <td colspan="6">${escapeHtml(mensagem)}</td>
    </tr>
  `;
}

function atualizarContador(resposta) {
  const contador = qs('contador-resultados');
  if (resposta.total === 0) {
    contador.textContent = 'Nenhum resultado.';
    return;
  }
  const inicio = resposta.offset + 1;
  const fim = Math.min(resposta.offset + resposta.items.length, resposta.total);
  contador.textContent = `Mostrando ${inicio}–${fim} de ${resposta.total} resultados`;
}

function atualizarPaginacao(resposta) {
  const anterior = qs('btn-anterior');
  const proxima = qs('btn-proxima');
  anterior.disabled = resposta.offset === 0;
  proxima.disabled = resposta.offset + resposta.items.length >= resposta.total;
}

function paginaAnterior() {
  if (state.paginaAtual === 0) return;
  state.paginaAtual -= 1;
  buscarResultados();
}

function paginaProxima() {
  state.paginaAtual += 1;
  buscarResultados();
}

function limparFiltros() {
  qs('filtro-execucao').value = '';
  qs('filtro-cenario').value = '';
  qs('filtro-ano-min').value = '';
  qs('filtro-ano-max').value = '';
  qs('filtro-uf').value = '';
  state.filtrosAtivos = {};
  state.paginaAtual = 0;
  qs('tbody-resultados').innerHTML = `
    <tr class="linha-vazia">
      <td colspan="6">Ajuste os filtros e clique em "Buscar" para carregar resultados.</td>
    </tr>
  `;
  qs('contador-resultados').textContent = '';
  qs('btn-anterior').disabled = true;
  qs('btn-proxima').disabled = true;
  destruirGrafico();
}

// -------------------------------------------------------------------------
// Gráfico (Chart.js)
// -------------------------------------------------------------------------

function renderizarGrafico(items) {
  const filtros = state.filtrosAtivos;
  const wrapper = qs('grafico-wrapper');
  if (!filtros.ano_min || !filtros.ano_max || !items || items.length === 0) {
    destruirGrafico();
    return;
  }
  const porAno = new Map();
  for (const item of items) {
    const registro = porAno.get(item.ano);
    if (registro) {
      registro.soma += item.frequencia_dias_secos_quentes;
      registro.n += 1;
    } else {
      porAno.set(item.ano, {
        soma: item.frequencia_dias_secos_quentes,
        n: 1,
      });
    }
  }
  const anos = Array.from(porAno.keys()).sort((a, b) => a - b);
  if (anos.length < 3) {
    destruirGrafico();
    return;
  }
  const dados = anos.map((ano) => {
    const r = porAno.get(ano);
    return r.soma / r.n;
  });

  wrapper.hidden = false;
  const canvas = qs('grafico-anos');
  if (state.grafico) {
    state.grafico.data.labels = anos;
    state.grafico.data.datasets[0].data = dados;
    state.grafico.update();
    return;
  }
  if (typeof Chart === 'undefined') {
    // CDN indisponível; esconde o bloco silenciosamente.
    wrapper.hidden = true;
    return;
  }
  state.grafico = new Chart(canvas, {
    type: 'line',
    data: {
      labels: anos,
      datasets: [
        {
          label: 'Frequência média (dias secos quentes)',
          data: dados,
          borderColor: '#0369a1',
          backgroundColor: 'rgba(3, 105, 161, 0.15)',
          tension: 0.25,
          pointRadius: 3,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: true, position: 'bottom' },
      },
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

function destruirGrafico() {
  qs('grafico-wrapper').hidden = true;
  if (state.grafico) {
    state.grafico.destroy();
    state.grafico = null;
  }
}

// -------------------------------------------------------------------------
// Boot
// -------------------------------------------------------------------------

function popularSelectUf() {
  const select = qs('filtro-uf');
  for (const uf of UFS) {
    const opt = document.createElement('option');
    opt.value = uf;
    opt.textContent = uf;
    select.appendChild(opt);
  }
}

function registrarEventos() {
  qs('form-execucao').addEventListener('submit', criarExecucao);
  qs('form-filtros').addEventListener('submit', submeterBusca);
  qs('btn-limpar').addEventListener('click', limparFiltros);
  qs('btn-anterior').addEventListener('click', paginaAnterior);
  qs('btn-proxima').addEventListener('click', paginaProxima);
}

document.addEventListener('DOMContentLoaded', () => {
  popularSelectUf();
  registrarEventos();
});
