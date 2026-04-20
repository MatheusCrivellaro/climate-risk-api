export interface EntradaCidadeUf {
  cidade: string;
  uf: string;
}

export function parseCidadeUfText(text: string): EntradaCidadeUf[] {
  const linhas = text.split(/\r?\n/);
  const out: EntradaCidadeUf[] = [];
  for (const linha of linhas) {
    const bruta = linha.trim();
    if (!bruta || !bruta.includes('/')) continue;
    const [cidadeRaw, ufRaw] = bruta.split('/');
    const cidade = (cidadeRaw ?? '').trim();
    const uf = (ufRaw ?? '').trim().toUpperCase();
    if (!cidade || uf.length !== 2) continue;
    out.push({ cidade, uf });
  }
  return out;
}
