// Página /estudo/ — JavaScript vanilla.
// Slice 17: 6 pastas (3 vars × 2 cenários) → POST /em-lote → 2 execuções
// independentes com polling paralelo. Consulta de resultados igual à
// Slice 16.

'use strict';

const API_BASE = '/api';
const POLLING_INTERVAL_MS = 3000;
const PAGINA_TAMANHO = 50;
const STATUSES_FINAIS = new Set(['completed', 'failed', 'canceled']);
const CENARIOS = ['rcp45', 'rcp85'];

const UFS = [
  'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
  'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
  'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO',
];

const state = {
  // Map<cenario, {execucao_id, statusAtual, jobId, erro?}>
  execucoesAtivas: new Map(),
  pollingTimers: new Map(), // Map<cenario, intervalId>
  paginaAtual: 0,
  totalResultados: 0,
  filtrosAtivos: {},
  grafico: null,
};

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

function qs(id) {
  return document.getElementById(id);
}

function formatarNumero(valor, casas = 2) {
  if (valor === null || valor === undefined || Number.isNaN(valor)) return '—';
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

function limparAspasDuplas(valor) {
  // O "Copiar como caminho" do Windows insere aspas no início e fim.
  return valor.replace(/^"+|"+$/g, '').trim();
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
// Seção 1: criar execuções em lote + polling paralelo
// -------------------------------------------------------------------------

function lerCampoPasta(idPrefixo, sufixo) {
  return limparAspasDuplas(qs(`${idPrefixo}-pasta-${sufixo}`).value);
}

async function criarExecucoes(event) {
  event.preventDefault();
  pararTodosPollings();
  state.execucoesAtivas = new Map();
  qs('status-execucoes').innerHTML = '';

  const erroGlobal = qs('erro-global');
  erroGlobal.className = 'feedback';
  erroGlobal.textContent = '';

  const params = {
    limiar_pr_mm_dia: Number(qs('limiar-pr').value) || 1.0,
    limiar_tas_c: Number(qs('limiar-tas').value) || 30.0,
  };

  const body = {
    rcp45: {
      pasta_pr: lerCampoPasta('rcp45', 'pr'),
      pasta_tas: lerCampoPasta('rcp45', 'tas'),
      pasta_evap: lerCampoPasta('rcp45', 'evap'),
    },
    rcp85: {
      pasta_pr: lerCampoPasta('rcp85', 'pr'),
      pasta_tas: lerCampoPasta('rcp85', 'tas'),
      pasta_evap: lerCampoPasta('rcp85', 'evap'),
    },
    parametros: params,
  };

  const camposVazios = [];
  for (const cenario of CENARIOS) {
    for (const campo of ['pasta_pr', 'pasta_tas', 'pasta_evap']) {
      if (!body[cenario][campo]) {
        camposVazios.push(`${cenario}.${campo}`);
      }
    }
  }
  if (camposVazios.length > 0) {
    erroGlobal.className = 'feedback feedback-erro';
    erroGlobal.textContent = `Preencha todas as 6 pastas (faltando: ${camposVazios.join(', ')}).`;
    return;
  }

  const botao = qs('btn-criar');
  botao.disabled = true;
  botao.textContent = 'Criando...';

  try {
    const resposta = await chamarApi('/execucoes/estresse-hidrico/em-lote', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    for (const item of resposta.execucoes) {
      state.execucoesAtivas.set(item.cenario, {
        execucaoId: item.execucao_id,
        statusAtual: item.status,
        jobId: item.job_id,
        erro: item.erro,
      });
    }
    renderizarStatusExecucoes();
    iniciarPollingMultiplo();
  } catch (erro) {
    erroGlobal.className = 'feedback feedback-erro';
    erroGlobal.textContent = erro.message || 'Falha ao criar execuções.';
  } finally {
    botao.disabled = false;
    botao.textContent = 'Criar execuções';
  }
}

function renderizarStatusExecucoes() {
  const wrapper = qs('status-execucoes');
  if (state.execucoesAtivas.size === 0) {
    wrapper.innerHTML = '';
    return;
  }
  const html = CENARIOS.map((cenario) => {
    const info = state.execucoesAtivas.get(cenario);
    if (!info) return '';
    if (info.erro) {
      return `
        <div class="status-execucao status-execucao--${cenario} status-execucao--erro">
          <h4>${escapeHtml(cenario)}</h4>
          <div class="meta">
            <span>Falha:</span>
            <span>${escapeHtml(info.erro)}</span>
          </div>
        </div>
      `;
    }
    const id = info.execucaoId ?? '—';
    const statusBadge = info.statusAtual
      ? `<span class="status-badge status-${escapeHtml(info.statusAtual)}">${escapeHtml(info.statusAtual)}</span>`
      : '';
    return `
      <div class="status-execucao status-execucao--${cenario}">
        <h4>${escapeHtml(cenario)}</h4>
        <div class="meta">
          <code>${escapeHtml(id)}</code>
          ${statusBadge}
        </div>
      </div>
    `;
  }).join('');
  wrapper.innerHTML = html;
}

function iniciarPollingMultiplo() {
  for (const [cenario, info] of state.execucoesAtivas.entries()) {
    if (info.erro || !info.execucaoId) continue;
    if (info.statusAtual && STATUSES_FINAIS.has(info.statusAtual)) continue;
    const timer = setInterval(
      () => atualizarStatusCenario(cenario),
      POLLING_INTERVAL_MS,
    );
    state.pollingTimers.set(cenario, timer);
  }
  if (state.pollingTimers.size === 0) {
    aoTerminarTodosOsPollings();
  }
}

function pararPolling(cenario) {
  const timer = state.pollingTimers.get(cenario);
  if (timer !== undefined) {
    clearInterval(timer);
    state.pollingTimers.delete(cenario);
  }
}

function pararTodosPollings() {
  for (const cenario of [...state.pollingTimers.keys()]) {
    pararPolling(cenario);
  }
}

async function atualizarStatusCenario(cenario) {
  const info = state.execucoesAtivas.get(cenario);
  if (!info || !info.execucaoId) {
    pararPolling(cenario);
    return;
  }
  try {
    const resposta = await chamarApi(
      `/execucoes/${encodeURIComponent(info.execucaoId)}`,
    );
    info.statusAtual = resposta.status;
    state.execucoesAtivas.set(cenario, info);
    renderizarStatusExecucoes();
    if (STATUSES_FINAIS.has(resposta.status)) {
      pararPolling(cenario);
      if (state.pollingTimers.size === 0) {
        aoTerminarTodosOsPollings();
      }
    }
  } catch (erro) {
    pararPolling(cenario);
    info.erro = erro.message || String(erro);
    state.execucoesAtivas.set(cenario, info);
    renderizarStatusExecucoes();
  }
}

function aoTerminarTodosOsPollings() {
  // Quando todas as execuções terminaram, descer para resultados.
  // Usa só filtro de cenário/anos, sem fixar execucao_id (são duas).
  const algumaCompleted = [...state.execucoesAtivas.values()].some(
    (info) => info.statusAtual === 'completed',
  );
  if (!algumaCompleted) return;
  qs('filtro-execucao').value = '';
  qs('filtro-cenario').value = '';
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
  qs('tbody-resultados').innerHTML = '';

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
  qs('tbody-resultados').innerHTML = `
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
  qs('btn-anterior').disabled = resposta.offset === 0;
  qs('btn-proxima').disabled =
    resposta.offset + resposta.items.length >= resposta.total;
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
// Gráfico
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
  const anos = [...porAno.keys()].sort((a, b) => a - b);
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
      plugins: { legend: { display: true, position: 'bottom' } },
      scales: { y: { beginAtZero: true } },
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
  qs('form-criar-execucoes').addEventListener('submit', criarExecucoes);
  qs('form-filtros').addEventListener('submit', submeterBusca);
  qs('btn-limpar').addEventListener('click', limparFiltros);
  qs('btn-anterior').addEventListener('click', paginaAnterior);
  qs('btn-proxima').addEventListener('click', paginaProxima);
}

document.addEventListener('DOMContentLoaded', () => {
  popularSelectUf();
  registrarEventos();
});
