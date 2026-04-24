import { describe, expect, it } from 'vitest';
import { parseCidadeUfText } from './parseCidadeUf';

describe('parseCidadeUfText', () => {
  it('parses one entry per line in CIDADE/UF format', () => {
    const out = parseCidadeUfText('São Paulo/SP\nRio de Janeiro/RJ');
    expect(out).toEqual([
      { cidade: 'São Paulo', uf: 'SP' },
      { cidade: 'Rio de Janeiro', uf: 'RJ' },
    ]);
  });

  it('normalizes UF to uppercase and trims whitespace', () => {
    const out = parseCidadeUfText('  Campinas  /  sp  ');
    expect(out).toEqual([{ cidade: 'Campinas', uf: 'SP' }]);
  });

  it('skips blank lines and lines without slash', () => {
    const out = parseCidadeUfText('São Paulo/SP\n\nalgo\nRio/RJ');
    expect(out).toEqual([
      { cidade: 'São Paulo', uf: 'SP' },
      { cidade: 'Rio', uf: 'RJ' },
    ]);
  });

  it('drops invalid UF lengths', () => {
    const out = parseCidadeUfText('Foo/ABC\nBar/S\nBaz/SP');
    expect(out).toEqual([{ cidade: 'Baz', uf: 'SP' }]);
  });
});
