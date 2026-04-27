// Página /estudo/ — JavaScript vanilla.
// Slice 20.2: abas, modal browser de pastas, validação inline e downloads.
// Slice 20.3: persistência de estado em localStorage.

'use strict';

// ============================================================================
// Constantes
// ============================================================================

const API = {
  FS_LISTAR: '/api/fs/listar',
  EXECUCOES_EM_LOTE: '/api/execucoes/estresse-hidrico/em-lote',
  EXECUCAO_GET: (id) => `/api/execucoes/${encodeURIComponent(id)}`,
  RESULTADOS: '/api/resultados/estresse-hidrico',
  RESULTADOS_EXPORT: '/api/resultados/estresse-hidrico/export',
};

const POLLING_INTERVAL_MS = 3000;
const PAGE_SIZE = 50;
const VALIDACAO_DEBOUNCE_MS = 500;
const STATUSES_FINAIS = new Set(['completed', 'failed', 'canceled']);
const LIMITE_HISTORICO_EXECUCOES = 50;

// IMPORTANTE: ao mudar a estrutura do estado persistido,
// incrementar o sufixo de STORAGE_KEY (v1 → v2 → ...).
// Isso garante que estados antigos sejam ignorados em vez de causar bugs.
const STORAGE_KEY = 'climate_risk:estudo:state:v1';

// ============================================================================
// Persistência (localStorage)
// ============================================================================

const Persistencia = {
  carregar() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') return null;
      return parsed;
    } catch (e) {
      console.warn('Falha ao carregar estado persistido:', e);
      return null;
    }
  },

  salvar(estado) {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(estado));
    } catch (e) {
      console.warn('Falha ao salvar estado:', e);
    }
  },

  limpar() {
    localStorage.removeItem(STORAGE_KEY);
  },
};

function estadoVazio() {
  return {
    versao: 1,
    formulario_pastas: {
      rcp45: { pr: '', tas: '', evap: '' },
      rcp85: { pr: '', tas: '', evap: '' },
    },
    execucoes: [],
    ultimos_filtros: null,
    ultima_aba: null,
  };
}

function persistirCampoPasta(cenario, variavel, valor) {
  const estado = Persistencia.carregar() || estadoVazio();
  estado.formulario_pastas = estado.formulario_pastas || {
    rcp45: { pr: '', tas: '', evap: '' },
    rcp85: { pr: '', tas: '', evap: '' },
  };
  estado.formulario_pastas[cenario] = estado.formulario_pastas[cenario] || {
    pr: '',
    tas: '',
    evap: '',
  };
  estado.formulario_pastas[cenario][variavel] = valor;
  Persistencia.salvar(estado);
}

function persistirExecucoes(execucoes) {
  const estado = Persistencia.carregar() || estadoVazio();
  estado.execucoes = estado.execucoes || [];
  for (const exec of execucoes) {
    if (!exec.id) continue;
    estado.execucoes.push({
      execucao_id: exec.id,
      cenario: exec.cenario,
      criado_em: new Date().toISOString(),
      ultimo_status: exec.status || 'pending',
    });
  }
  if (estado.execucoes.length > LIMITE_HISTORICO_EXECUCOES) {
    estado.execucoes = estado.execucoes.slice(-LIMITE_HISTORICO_EXECUCOES);
  }
  Persistencia.salvar(estado);
}

function atualizarStatusPersistido(execucaoId, novoStatus) {
  const estado = Persistencia.carregar();
  if (!estado || !estado.execucoes) return;
  const exec = estado.execucoes.find((e) => e.execucao_id === execucaoId);
  if (exec && exec.ultimo_status !== novoStatus) {
    exec.ultimo_status = novoStatus;
    Persistencia.salvar(estado);
  }
}

function persistirFiltros() {
  const estado = Persistencia.carregar() || estadoVazio();
  estado.ultimos_filtros = {
    execucao_id: qs('filtro-execucao').value,
    cenario: qs('filtro-cenario').value,
    ano_min: parseInt(qs('filtro-ano-min').value, 10) || null,
    ano_max: parseInt(qs('filtro-ano-max').value, 10) || null,
    municipio_id: null,
    uf: qs('filtro-uf').value,
  };
  Persistencia.salvar(estado);
}

function persistirAba(nome) {
  const estado = Persistencia.carregar() || estadoVazio();
  estado.ultima_aba = nome;
  Persistencia.salvar(estado);
}

const UFS = [
  'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
  'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
  'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO',
];

// ============================================================================
// Estado em memória
// ============================================================================

const state = {
  abaAtiva: 'nova',
  execucoesAtivas: [],
  pollingTimer: null,
  resultados: [],
  totalResultados: 0,
  paginaAtual: 0,
  filtrosAtuais: {},
  modalContexto: null,
  caminhoModalAtual: null,
  arquivosModalAtual: [],
  grafico: null,
  resultadosNovosDisponiveis: false,
};

// ============================================================================
// Helpers genéricos
// ============================================================================

function qs(id) {
  return document.getElementById(id);
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

function formatarNumero(valor, casas = 2) {
  if (valor === null || valor === undefined || Number.isNaN(valor)) {
    return '—';
  }
  return Number(valor).toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  });
}

function debounce(fn, ms) {
  let timer = null;
  return (...args) => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

function limparAspas(valor) {
  return (valor || '').trim().replace(/^"+|"+$/g, '');
}

async function chamarApi(url, options = {}) {
  const resposta = await fetch(url, {
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

// ============================================================================
// Tabs
// ============================================================================

function inicializarTabs() {
  const botoes = document.querySelectorAll('.tab-button');
  botoes.forEach((botao) => {
    botao.addEventListener('click', () => trocarAba(botao.dataset.tab));
    botao.addEventListener('keydown', (e) => onKeyDownTabs(e, botoes, botao));
  });

  const hash = window.location.hash.replace('#', '');
  if (hash === 'resultados' || hash === 'nova') {
    trocarAba(hash, { atualizarHash: false });
  }
}

function onKeyDownTabs(event, botoes, atual) {
  const teclas = {
    ArrowRight: 1,
    ArrowLeft: -1,
  };
  if (!(event.key in teclas)) return;
  event.preventDefault();
  const lista = Array.from(botoes);
  const indiceAtual = lista.indexOf(atual);
  const novo = (indiceAtual + teclas[event.key] + lista.length) % lista.length;
  lista[novo].focus();
  trocarAba(lista[novo].dataset.tab);
}

function trocarAba(nome, { atualizarHash = true } = {}) {
  if (nome !== 'nova' && nome !== 'resultados') return;
  state.abaAtiva = nome;

  document.querySelectorAll('.tab-button').forEach((botao) => {
    const ativo = botao.dataset.tab === nome;
    botao.setAttribute('aria-selected', ativo ? 'true' : 'false');
    botao.classList.toggle('tab-ativa', ativo);
    botao.tabIndex = ativo ? 0 : -1;
  });

  document.querySelectorAll('.tab-panel').forEach((panel) => {
    panel.hidden = panel.id !== `tab-${nome}`;
  });

  if (atualizarHash) {
    history.replaceState(null, '', `#${nome}`);
  }

  if (nome === 'resultados' && state.resultadosNovosDisponiveis) {
    state.resultadosNovosDisponiveis = false;
    qs('badge-novos-resultados').hidden = true;
  }

  persistirAba(nome);
}

// ============================================================================
// Modal browser de pastas
// ============================================================================

function abrirModalPastas(targetInputId) {
  state.modalContexto = targetInputId;
  const input = qs(targetInputId);
  const cenarioEsperado = input ? input.dataset.cenario : null;
  const titulo = qs('modal-titulo');
  if (cenarioEsperado) {
    titulo.textContent = `Selecionar pasta — ${cenarioEsperado}`;
  } else {
    titulo.textContent = 'Selecionar pasta';
  }

  const modal = qs('modal-browser-pastas');
  if (typeof modal.showModal === 'function') {
    modal.showModal();
  } else {
    modal.setAttribute('open', '');
  }

  const valorAtual = limparAspas(input ? input.value : '');
  carregarPasta(valorAtual || null).catch((erro) => {
    renderizarErroModal(erro.message || 'Erro ao listar pasta.');
  });
}

function fecharModal() {
  const modal = qs('modal-browser-pastas');
  if (typeof modal.close === 'function') {
    modal.close();
  } else {
    modal.removeAttribute('open');
  }
  state.modalContexto = null;
  state.caminhoModalAtual = null;
  state.arquivosModalAtual = [];
}

async function carregarPasta(caminho) {
  const lista = qs('modal-lista-pastas');
  lista.innerHTML = '<li class="lista-vazia">Carregando…</li>';
  qs('modal-cenario-detectado').hidden = true;

  const url = new URL(API.FS_LISTAR, window.location.origin);
  if (caminho) url.searchParams.set('caminho', caminho);

  let resposta;
  try {
    resposta = await chamarApi(url.toString());
  } catch (erro) {
    if (caminho) {
      // Fallback: caminho inválido → carrega raiz
      return carregarPasta(null);
    }
    throw erro;
  }

  state.caminhoModalAtual = resposta.caminho_atual;
  state.arquivosModalAtual = resposta.arquivos_nc || [];

  renderizarBreadcrumb(resposta);
  renderizarConteudoPasta(resposta);
  renderizarCenarioDetectado(resposta);
}

function renderizarBreadcrumb(resposta) {
  const breadcrumb = qs('modal-breadcrumb');
  const partes = [];

  partes.push(
    `<button type="button" data-caminho="${escapeHtml(resposta.pasta_raiz)}">📂 raiz</button>`,
  );

  const relativo = resposta.caminho_relativo_raiz;
  if (relativo && relativo !== '.') {
    const segmentos = relativo.split(/[\\/]/).filter(Boolean);
    let acumulado = resposta.pasta_raiz;
    const sep = resposta.pasta_raiz.includes('\\') ? '\\' : '/';
    segmentos.forEach((seg, i) => {
      acumulado = `${acumulado}${sep}${seg}`;
      partes.push('<span class="breadcrumb-separator">/</span>');
      if (i === segmentos.length - 1) {
        partes.push(`<span class="breadcrumb-atual">${escapeHtml(seg)}</span>`);
      } else {
        partes.push(
          `<button type="button" data-caminho="${escapeHtml(acumulado)}">${escapeHtml(seg)}</button>`,
        );
      }
    });
  }

  breadcrumb.innerHTML = partes.join('');
  breadcrumb.querySelectorAll('button[data-caminho]').forEach((btn) => {
    btn.addEventListener('click', () => {
      carregarPasta(btn.dataset.caminho).catch((erro) =>
        renderizarErroModal(erro.message),
      );
    });
  });
}

function renderizarConteudoPasta(resposta) {
  const lista = qs('modal-lista-pastas');
  const items = [];

  if (resposta.pode_subir && resposta.pasta_pai) {
    items.push({
      tipo: 'pai',
      caminho: resposta.pasta_pai,
      nome: '..',
    });
  }

  for (const sub of resposta.subpastas || []) {
    items.push({
      tipo: 'pasta',
      caminho: sub.caminho_absoluto,
      nome: sub.nome,
      meta: `${sub.quantidade_nc} .nc`,
    });
  }

  for (const arq of resposta.arquivos_nc || []) {
    items.push({
      tipo: 'arquivo',
      nome: arq.nome,
      cenario: arq.cenario_detectado,
    });
  }

  if (items.length === 0) {
    lista.innerHTML = '<li class="lista-vazia">Pasta vazia.</li>';
    return;
  }

  const html = items
    .map((item) => {
      if (item.tipo === 'pai') {
        return `
          <li tabindex="0" data-caminho="${escapeHtml(item.caminho)}" data-tipo="pasta">
            <span class="item-icone" aria-hidden="true">⬆️</span>
            <span class="item-nome">${escapeHtml(item.nome)}</span>
            <span class="item-meta">subir</span>
          </li>
        `;
      }
      if (item.tipo === 'pasta') {
        return `
          <li tabindex="0" data-caminho="${escapeHtml(item.caminho)}" data-tipo="pasta">
            <span class="item-icone" aria-hidden="true">📁</span>
            <span class="item-nome">${escapeHtml(item.nome)}</span>
            <span class="item-meta">${escapeHtml(item.meta)}</span>
          </li>
        `;
      }
      return `
        <li class="item-arquivo">
          <span class="item-icone" aria-hidden="true">📄</span>
          <span class="item-nome">${escapeHtml(item.nome)}</span>
          ${item.cenario ? `<span class="item-meta">${escapeHtml(item.cenario)}</span>` : ''}
        </li>
      `;
    })
    .join('');
  lista.innerHTML = html;

  lista.querySelectorAll('li[data-tipo="pasta"]').forEach((li) => {
    const navegar = () => {
      carregarPasta(li.dataset.caminho).catch((erro) =>
        renderizarErroModal(erro.message),
      );
    };
    li.addEventListener('click', navegar);
    li.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        navegar();
      }
    });
  });
}

function renderizarCenarioDetectado(resposta) {
  const div = qs('modal-cenario-detectado');
  const arquivos = resposta.arquivos_nc || [];
  const cenarioEsperado = state.modalContexto
    ? qs(state.modalContexto)?.dataset.cenario
    : null;

  if (arquivos.length === 0) {
    div.hidden = true;
    return;
  }

  const cenarios = arquivos
    .map((a) => a.cenario_detectado)
    .filter((c) => c);
  const cenariosUnicos = [...new Set(cenarios)];

  div.hidden = false;
  if (cenariosUnicos.length === 0) {
    div.className = 'cenario-detectado cenario-info';
    div.innerHTML = `<span aria-hidden="true">ℹ️</span> ${arquivos.length} arquivos .nc (cenário não detectado)`;
    return;
  }

  if (cenarioEsperado && !cenariosUnicos.includes(cenarioEsperado)) {
    div.className = 'cenario-detectado cenario-aviso';
    div.innerHTML = `<span aria-hidden="true">⚠️</span> ${arquivos.length} arquivos detectados como ${cenariosUnicos.join(', ')} — esperado ${escapeHtml(cenarioEsperado)}`;
    return;
  }

  div.className = 'cenario-detectado cenario-ok';
  const rotulo = cenariosUnicos.length === 1 ? cenariosUnicos[0] : cenariosUnicos.join(', ');
  div.innerHTML = `<span aria-hidden="true">✓</span> ${arquivos.length} arquivos ${escapeHtml(rotulo)}`;
}

function renderizarErroModal(mensagem) {
  const lista = qs('modal-lista-pastas');
  lista.innerHTML = `<li class="lista-vazia">${escapeHtml(mensagem)}</li>`;
  qs('modal-cenario-detectado').hidden = true;
}

function selecionarPastaAtual() {
  if (!state.modalContexto || !state.caminhoModalAtual) {
    fecharModal();
    return;
  }
  const input = qs(state.modalContexto);
  if (input) {
    input.value = state.caminhoModalAtual;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    validarPastaDebounced(input);
  }
  fecharModal();
}

// ============================================================================
// Validação inline de pastas
// ============================================================================

function definirFeedback(input, classe, mensagem) {
  const feedback = qs(`feedback-${input.id}`);
  if (!feedback) return;
  input.classList.remove('input-ok', 'input-aviso', 'input-erro');
  feedback.classList.remove('feedback-ok', 'feedback-aviso', 'feedback-erro');
  if (!classe) {
    feedback.textContent = '';
    return;
  }
  input.classList.add(`input-${classe}`);
  feedback.classList.add(`feedback-${classe}`);
  feedback.textContent = mensagem;
}

const validarPastaDebounced = debounce(async (input) => {
  const valor = limparAspas(input.value);
  if (!valor) {
    definirFeedback(input, null, '');
    return;
  }
  const url = new URL(API.FS_LISTAR, window.location.origin);
  url.searchParams.set('caminho', valor);
  try {
    const resposta = await chamarApi(url.toString());
    const arquivos = resposta.arquivos_nc || [];
    const cenarioEsperado = input.dataset.cenario;
    if (arquivos.length === 0) {
      definirFeedback(input, 'aviso', '⚠ Nenhum arquivo .nc na pasta');
      return;
    }
    const cenarios = arquivos.map((a) => a.cenario_detectado).filter((c) => c);
    const cenariosUnicos = [...new Set(cenarios)];
    if (cenariosUnicos.length === 0) {
      definirFeedback(
        input,
        'ok',
        `✓ ${arquivos.length} arquivo(s) .nc encontrado(s)`,
      );
      return;
    }
    if (cenarioEsperado && !cenariosUnicos.includes(cenarioEsperado)) {
      definirFeedback(
        input,
        'aviso',
        `⚠ Detectado ${cenariosUnicos.join(', ')} — esperado ${cenarioEsperado}`,
      );
      return;
    }
    definirFeedback(
      input,
      'ok',
      `✓ ${arquivos.length} arquivo(s) ${cenariosUnicos.join(', ')}`,
    );
  } catch (erro) {
    const msg = erro.message || 'Pasta inválida';
    definirFeedback(input, 'erro', `✗ ${msg}`);
  }
}, VALIDACAO_DEBOUNCE_MS);

// ============================================================================
// Submit do formulário (criar execuções em lote)
// ============================================================================

function lerPastasDoForm(cenario) {
  return {
    pasta_pr: limparAspas(qs(`${cenario}-pasta-pr`).value),
    pasta_tas: limparAspas(qs(`${cenario}-pasta-tas`).value),
    pasta_evap: limparAspas(qs(`${cenario}-pasta-evap`).value),
  };
}

async function criarExecucoes(event) {
  event.preventDefault();
  pararPolling();

  const feedback = qs('feedback-criar');
  feedback.className = 'feedback-form';
  feedback.textContent = '';

  const rcp45 = lerPastasDoForm('rcp45');
  const rcp85 = lerPastasDoForm('rcp85');

  const todas = [
    rcp45.pasta_pr, rcp45.pasta_tas, rcp45.pasta_evap,
    rcp85.pasta_pr, rcp85.pasta_tas, rcp85.pasta_evap,
  ];
  if (todas.some((v) => !v)) {
    feedback.className = 'feedback-form feedback-erro';
    feedback.textContent = 'Informe as 6 pastas (3 variáveis × 2 cenários).';
    return;
  }

  const dados = {
    rcp45,
    rcp85,
    parametros: {
      limiar_pr_mm_dia: Number(qs('limiar-pr').value),
      limiar_tas_c: Number(qs('limiar-tas').value),
    },
  };

  const botao = qs('btn-criar');
  botao.disabled = true;
  botao.textContent = 'Criando…';

  try {
    const resposta = await chamarApi(API.EXECUCOES_EM_LOTE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(dados),
    });
    state.execucoesAtivas = (resposta.execucoes || []).map((item) => ({
      cenario: item.cenario,
      id: item.execucao_id || null,
      jobId: item.job_id || null,
      status: item.status || (item.erro ? 'failed' : 'pending'),
      erro: item.erro || null,
    }));
    persistirExecucoes(state.execucoesAtivas);
    renderizarStatusExecucoes();
    iniciarPolling();
  } catch (erro) {
    feedback.className = 'feedback-form feedback-erro';
    feedback.textContent = erro.message || 'Falha ao criar execuções.';
  } finally {
    botao.disabled = false;
    botao.textContent = 'Criar execuções';
  }
}

function renderizarStatusExecucoes() {
  const secao = qs('status-execucoes');
  const cards = qs('cards-status');
  if (state.execucoesAtivas.length === 0) {
    secao.hidden = true;
    cards.innerHTML = '';
    return;
  }
  secao.hidden = false;
  const html = state.execucoesAtivas
    .map((exec) => {
      const rotulo = `<span class="cenario-rotulo rotulo-${escapeHtml(exec.cenario)}">${escapeHtml(exec.cenario)}</span>`;
      if (exec.erro) {
        return `
          <div class="card-status card-status-erro">
            ${rotulo}
            <span>Falha: ${escapeHtml(exec.erro)}</span>
          </div>
        `;
      }
      const idCurto = exec.id ? `<code>${escapeHtml(exec.id)}</code>` : '';
      const status = exec.status || 'pending';
      return `
        <div class="card-status">
          ${rotulo}
          ${idCurto}
          <span class="status-badge status-${escapeHtml(status)}">${escapeHtml(status)}</span>
        </div>
      `;
    })
    .join('');
  cards.innerHTML = html;
}

// ============================================================================
// Polling
// ============================================================================

function iniciarPolling() {
  pararPolling();
  if (todasFinalizadas()) {
    onExecucoesFinalizadas();
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
  if (state.execucoesAtivas.length === 0) return false;
  return state.execucoesAtivas.every(
    (exec) => exec.erro || STATUSES_FINAIS.has(exec.status),
  );
}

async function atualizarStatusExecucoes() {
  const pendentes = state.execucoesAtivas.filter(
    (exec) => exec.id && !exec.erro && !STATUSES_FINAIS.has(exec.status),
  );
  if (pendentes.length === 0) {
    pararPolling();
    onExecucoesFinalizadas();
    return;
  }
  await Promise.all(pendentes.map((exec) => atualizarStatusUmaExecucao(exec)));
  renderizarStatusExecucoes();
  if (todasFinalizadas()) {
    pararPolling();
    onExecucoesFinalizadas();
  }
}

async function atualizarStatusUmaExecucao(exec) {
  try {
    const resposta = await chamarApi(API.EXECUCAO_GET(exec.id));
    exec.status = resposta.status;
  } catch (erro) {
    exec.erro = erro.message || String(erro);
    exec.status = 'failed';
  }
  if (exec.id) {
    atualizarStatusPersistido(exec.id, exec.status);
  }
}

function onExecucoesFinalizadas() {
  const algumaCompletou = state.execucoesAtivas.some(
    (exec) => exec.status === 'completed',
  );
  if (algumaCompletou && state.abaAtiva !== 'resultados') {
    state.resultadosNovosDisponiveis = true;
    qs('badge-novos-resultados').hidden = false;
  }
}

// ============================================================================
// Resultados
// ============================================================================

function lerFiltrosDoForm() {
  const filtros = {};
  const execucao = qs('filtro-execucao').value.trim();
  const cenario = qs('filtro-cenario').value;
  const anoMin = qs('filtro-ano-min').value.trim();
  const anoMax = qs('filtro-ano-max').value.trim();
  const uf = qs('filtro-uf').value;
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
  const str = params.toString();
  return str ? `?${str}` : '';
}

async function buscarResultados() {
  state.filtrosAtuais = lerFiltrosDoForm();
  state.paginaAtual = 0;
  await carregarPagina();
}

async function carregarPagina() {
  const contador = qs('contador-resultados');
  contador.textContent = 'Carregando…';
  const tbody = qs('tbody-resultados');
  tbody.innerHTML = '';

  const queryString = montarQueryString(state.filtrosAtuais, {
    limit: PAGE_SIZE,
    offset: state.paginaAtual * PAGE_SIZE,
  });

  try {
    const resposta = await chamarApi(`${API.RESULTADOS}${queryString}`);
    state.totalResultados = resposta.total;
    state.resultados = resposta.items;
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
  tbody.innerHTML = `<tr class="linha-vazia"><td colspan="6">${escapeHtml(mensagem)}</td></tr>`;
}

function atualizarContador(resposta) {
  const contador = qs('contador-resultados');
  if (resposta.total === 0) {
    contador.textContent = 'Nenhum resultado.';
    return;
  }
  const inicio = resposta.offset + 1;
  const fim = Math.min(resposta.offset + resposta.items.length, resposta.total);
  contador.textContent = `Mostrando ${inicio}–${fim} de ${resposta.total}`;
}

function atualizarPaginacao(resposta) {
  const anterior = qs('btn-pag-anterior');
  const proxima = qs('btn-pag-proxima');
  anterior.disabled = resposta.offset === 0;
  proxima.disabled = resposta.offset + resposta.items.length >= resposta.total;
  const info = qs('info-paginacao');
  if (resposta.total === 0) {
    info.textContent = '';
    return;
  }
  const totalPaginas = Math.max(1, Math.ceil(resposta.total / PAGE_SIZE));
  info.textContent = `Página ${state.paginaAtual + 1} de ${totalPaginas}`;
}

function paginaAnterior() {
  if (state.paginaAtual === 0) return;
  state.paginaAtual -= 1;
  carregarPagina();
}

function paginaProxima() {
  state.paginaAtual += 1;
  carregarPagina();
}

function limparFiltros() {
  qs('filtro-execucao').value = '';
  qs('filtro-cenario').value = '';
  qs('filtro-ano-min').value = '';
  qs('filtro-ano-max').value = '';
  qs('filtro-uf').value = '';
  persistirFiltros();
  state.filtrosAtuais = {};
  state.paginaAtual = 0;
  qs('tbody-resultados').innerHTML = `
    <tr class="linha-vazia">
      <td colspan="6">Ajuste os filtros e clique em "Buscar" para carregar resultados.</td>
    </tr>
  `;
  qs('contador-resultados').textContent = 'Nenhum resultado carregado';
  qs('btn-pag-anterior').disabled = true;
  qs('btn-pag-proxima').disabled = true;
  qs('info-paginacao').textContent = '';
  destruirGrafico();
}

// ============================================================================
// Gráfico (Chart.js)
// ============================================================================

function renderizarGrafico(items) {
  const filtros = state.filtrosAtuais;
  const wrapper = qs('grafico-frequencia-anual');
  if (!filtros.ano_min || !filtros.ano_max || !items || items.length === 0) {
    destruirGrafico();
    return;
  }
  const porAno = new Map();
  for (const item of items) {
    const reg = porAno.get(item.ano);
    if (reg) {
      reg.soma += item.frequencia_dias_secos_quentes;
      reg.n += 1;
    } else {
      porAno.set(item.ano, { soma: item.frequencia_dias_secos_quentes, n: 1 });
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
  const canvas = qs('canvas-grafico');
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
  qs('grafico-frequencia-anual').hidden = true;
  if (state.grafico) {
    state.grafico.destroy();
    state.grafico = null;
  }
}

// ============================================================================
// Downloads
// ============================================================================

function baixar(formato) {
  const params = new URLSearchParams();
  params.set('formato', formato);
  const baixarTudo = qs('export-tudo').checked;
  if (!baixarTudo) {
    Object.entries(state.filtrosAtuais).forEach(([k, v]) => {
      if (v !== '' && v !== null && v !== undefined) {
        params.set(k, String(v));
      }
    });
  }
  window.location.href = `${API.RESULTADOS_EXPORT}?${params}`;
}

// ============================================================================
// Boot
// ============================================================================

function popularSelectUFs() {
  const select = qs('filtro-uf');
  for (const uf of UFS) {
    const opt = document.createElement('option');
    opt.value = uf;
    opt.textContent = uf;
    select.appendChild(opt);
  }
}

function ligarEventListeners() {
  qs('form-criar-execucoes').addEventListener('submit', criarExecucoes);

  document.querySelectorAll('.btn-procurar').forEach((btn) => {
    btn.addEventListener('click', () => abrirModalPastas(btn.dataset.target));
  });

  document.querySelectorAll('input[data-cenario][data-variavel]').forEach((input) => {
    input.addEventListener('input', () => validarPastaDebounced(input));
    input.addEventListener('blur', () => validarPastaDebounced(input));
    input.addEventListener('change', () => {
      persistirCampoPasta(
        input.dataset.cenario,
        input.dataset.variavel,
        limparAspas(input.value),
      );
    });
  });

  qs('modal-btn-fechar').addEventListener('click', fecharModal);
  qs('modal-btn-cancelar').addEventListener('click', fecharModal);
  qs('modal-btn-selecionar').addEventListener('click', selecionarPastaAtual);
  qs('modal-browser-pastas').addEventListener('close', () => {
    state.modalContexto = null;
    state.caminhoModalAtual = null;
  });
  qs('modal-browser-pastas').addEventListener('click', (event) => {
    if (event.target === qs('modal-browser-pastas')) {
      fecharModal();
    }
  });

  qs('btn-buscar').addEventListener('click', buscarResultados);
  qs('btn-limpar-filtros').addEventListener('click', limparFiltros);
  qs('btn-pag-anterior').addEventListener('click', paginaAnterior);
  qs('btn-pag-proxima').addEventListener('click', paginaProxima);

  document.querySelectorAll('.btn-export').forEach((btn) => {
    btn.addEventListener('click', () => baixar(btn.dataset.formato));
  });

  ['filtro-execucao', 'filtro-cenario', 'filtro-ano-min', 'filtro-ano-max', 'filtro-uf']
    .forEach((id) => {
      const elem = qs(id);
      if (elem) elem.addEventListener('change', persistirFiltros);
    });

  qs('btn-limpar-historico').addEventListener('click', () => {
    if (window.confirm('Limpar histórico de execuções e formulário?')) {
      Persistencia.limpar();
      window.location.reload();
    }
  });
}

async function restaurarEstado() {
  const estado = Persistencia.carregar();
  if (!estado) return;

  if (estado.formulario_pastas) {
    for (const cenario of ['rcp45', 'rcp85']) {
      for (const variavel of ['pr', 'tas', 'evap']) {
        const valor = estado.formulario_pastas[cenario]?.[variavel];
        if (valor) {
          const input = qs(`${cenario}-pasta-${variavel}`);
          if (input) {
            input.value = valor;
            validarPastaDebounced(input);
          }
        }
      }
    }
  }

  if (estado.ultimos_filtros) {
    const map = {
      execucao_id: 'filtro-execucao',
      cenario: 'filtro-cenario',
      ano_min: 'filtro-ano-min',
      ano_max: 'filtro-ano-max',
      uf: 'filtro-uf',
    };
    for (const [key, value] of Object.entries(estado.ultimos_filtros)) {
      const id = map[key];
      if (!id) continue;
      const elem = qs(id);
      if (elem && value !== null && value !== undefined && value !== '') {
        elem.value = value;
      }
    }
  }

  if (estado.execucoes && estado.execucoes.length > 0) {
    await reconciliarExecucoesAtivas(estado.execucoes);
  }

  if (!window.location.hash && estado.ultima_aba) {
    trocarAba(estado.ultima_aba);
  }
}

async function reconciliarExecucoesAtivas(execucoesPersistidas) {
  const reconciliados = [];
  let temNovasCompleted = false;

  for (const exec of execucoesPersistidas) {
    if (!exec.execucao_id) continue;
    try {
      const resp = await fetch(API.EXECUCAO_GET(exec.execucao_id), {
        headers: { Accept: 'application/json' },
      });

      if (resp.status === 404) {
        continue;
      }

      if (!resp.ok) {
        reconciliados.push(exec);
        continue;
      }

      const data = await resp.json();
      const statusServidor = data.status;

      if (
        (exec.ultimo_status === 'pending' || exec.ultimo_status === 'running') &&
        statusServidor === 'completed'
      ) {
        temNovasCompleted = true;
      }

      exec.ultimo_status = statusServidor;
      reconciliados.push(exec);

      if (statusServidor === 'pending' || statusServidor === 'running') {
        state.execucoesAtivas.push({
          cenario: exec.cenario,
          id: exec.execucao_id,
          jobId: null,
          status: statusServidor,
          erro: null,
        });
      }
    } catch (e) {
      console.warn(`Falha ao reconciliar ${exec.execucao_id}:`, e);
      reconciliados.push(exec);
    }
  }

  const estado = Persistencia.carregar() || estadoVazio();
  estado.execucoes = reconciliados;
  Persistencia.salvar(estado);

  if (state.execucoesAtivas.length > 0) {
    qs('status-execucoes').hidden = false;
    renderizarStatusExecucoes();
    iniciarPolling();
  }

  if (temNovasCompleted && state.abaAtiva !== 'resultados') {
    state.resultadosNovosDisponiveis = true;
    const badge = qs('badge-novos-resultados');
    badge.hidden = false;
    badge.textContent = '●';
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  popularSelectUFs();
  ligarEventListeners();
  inicializarTabs();
  await restaurarEstado();
});
