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
  // Slice 17: até dois itens (rcp45 + rcp85), cada um com {cenario, id, jobId, status, erro}
  execucoesEmLote: [],
  pollingTimer: null,
  paginaAtual: 0,
  totalResultados: 0,
  filtrosAtivos: {},
  grafico: null,
  scrollFeito: false,
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

function lerPastasDoForm(form, cenario) {
  // Slice 17: limpeza obrigatória — remove aspas envolventes ("Copiar como
  // caminho" do Windows) antes de enviar.
  const limpar = (valor) => valor.trim().replace(/^"+|"+$/g, '');
  return {
    pasta_pr: limpar(form.elements[`${cenario}-pasta-pr`].value),
    pasta_tas: limpar(form.elements[`${cenario}-pasta-tas`].value),
    pasta_evap: limpar(form.elements[`${cenario}-pasta-evap`].value),
  };
}

async function criarExecucao(event) {
  event.preventDefault();
  pararPolling();

  const feedback = qs('feedback-criar');
  feedback.className = 'feedback';
  feedback.textContent = '';

  const form = event.currentTarget;
  const rcp45 = lerPastasDoForm(form, 'rcp45');
  const rcp85 = lerPastasDoForm(form, 'rcp85');

  const todas = [
    rcp45.pasta_pr, rcp45.pasta_tas, rcp45.pasta_evap,
    rcp85.pasta_pr, rcp85.pasta_tas, rcp85.pasta_evap,
  ];
  if (todas.some((v) => !v)) {
    mostrarFeedbackErro('Informe as 6 pastas (3 variáveis × 2 cenários).');
    return;
  }

  const dados = {
    rcp45,
    rcp85,
    parametros: {
      limiar_pr_mm_dia: Number(form.limiar_pr_mm_dia.value),
      limiar_tas_c: Number(form.limiar_tas_c.value),
    },
  };

  const botao = qs('btn-criar');
  botao.disabled = true;
  botao.textContent = 'Criando...';

  try {
    const resposta = await chamarApi('/execucoes/estresse-hidrico/em-lote', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dados),
    });
    state.execucoesEmLote = (resposta.execucoes || []).map((item) => ({
      cenario: item.cenario,
      id: item.execucao_id || null,
      jobId: item.job_id || null,
      status: item.status || (item.erro ? 'failed' : 'pending'),
      erro: item.erro || null,
    }));
    state.scrollFeito = false;
    renderizarFeedbackExecucao();
    iniciarPolling();
  } catch (erro) {
    mostrarFeedbackErro(erro.message || 'Falha ao criar execuções.');
  } finally {
    botao.disabled = false;
    botao.textContent = 'Criar execuções';
  }
}

function mostrarFeedbackErro(mensagem) {
  const feedback = qs('feedback-criar');
  feedback.className = 'feedback feedback-erro';
  feedback.textContent = mensagem;
}

function renderizarFeedbackExecucao() {
  const feedback = qs('feedback-criar');
  if (state.execucoesEmLote.length === 0) {
    feedback.className = 'feedback';
    feedback.textContent = '';
    return;
  }
  feedback.className = 'feedback feedback-info';
  const linhas = state.execucoesEmLote.map((exec) => {
    const rotulo = `<span class="execucao-rotulo rotulo-${escapeHtml(exec.cenario)}">${escapeHtml(exec.cenario)}</span>`;
    if (exec.erro) {
      return `
        <div class="execucao-em-lote execucao-erro">
          ${rotulo}
          <span>Falha: ${escapeHtml(exec.erro)}</span>
        </div>
      `;
    }
    const idCurto = exec.id ? `<code>${escapeHtml(exec.id)}</code>` : '';
    const status = exec.status || 'pending';
    return `
      <div class="execucao-em-lote">
        ${rotulo}
        ${idCurto}
        <span class="status-badge status-${escapeHtml(status)}">${escapeHtml(status)}</span>
      </div>
    `;
  });
  feedback.innerHTML = `<div class="execucoes-em-lote">${linhas.join('')}</div>`;
}

function iniciarPolling() {
  pararPolling();
  if (todasFinalizadas()) {
    talvezScrollarParaResultados();
    return;
  }
  state.pollingTimer = setInterval(atualizarStatusExecucoes, POLLING_INTERVAL_MS);
}

function pararPolling() {
  if (state.pollingTimer !== null) {
    clearInterval(state.pollingTimer);
    state.pollingTimer = null;
  }
}

function todasFinalizadas() {
  if (state.execucoesEmLote.length === 0) return false;
  return state.execucoesEmLote.every(
    (exec) => exec.erro || STATUSES_FINAIS.has(exec.status),
  );
}

async function atualizarStatusExecucoes() {
  const pendentes = state.execucoesEmLote.filter(
    (exec) => exec.id && !exec.erro && !STATUSES_FINAIS.has(exec.status),
  );
  if (pendentes.length === 0) {
    pararPolling();
    talvezScrollarParaResultados();
    return;
  }
  await Promise.all(pendentes.map((exec) => atualizarStatusUmaExecucao(exec)));
  renderizarFeedbackExecucao();
  if (todasFinalizadas()) {
    pararPolling();
    talvezScrollarParaResultados();
  }
}

async function atualizarStatusUmaExecucao(exec) {
  try {
    const resposta = await chamarApi(
      `/execucoes/${encodeURIComponent(exec.id)}`,
    );
    exec.status = resposta.status;
  } catch (erro) {
    exec.erro = erro.message || String(erro);
    exec.status = 'failed';
  }
}

function talvezScrollarParaResultados() {
  if (state.scrollFeito) return;
  state.scrollFeito = true;
  qs('resultados-titulo').scrollIntoView({ behavior: 'smooth', block: 'start' });
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
        <td>${formatarNumero(item.intensidade_mm_dia)}</td>
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
