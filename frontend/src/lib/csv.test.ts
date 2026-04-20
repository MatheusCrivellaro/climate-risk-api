import { describe, expect, it } from 'vitest';
import { toCSV, toCSVValue } from './csv';

describe('toCSVValue', () => {
  it('returns empty string for null/undefined', () => {
    expect(toCSVValue(null)).toBe('');
    expect(toCSVValue(undefined)).toBe('');
  });

  it('passes through simple strings and numbers', () => {
    expect(toCSVValue('abc')).toBe('abc');
    expect(toCSVValue(42)).toBe('42');
    expect(toCSVValue(true)).toBe('true');
  });

  it('quotes values with commas, newlines, quotes or semicolons', () => {
    expect(toCSVValue('a,b')).toBe('"a,b"');
    expect(toCSVValue('line1\nline2')).toBe('"line1\nline2"');
    expect(toCSVValue('say "hi"')).toBe('"say ""hi"""');
    expect(toCSVValue('a;b')).toBe('"a;b"');
  });
});

describe('toCSV', () => {
  it('renders header + rows with a trailing-newline-free body', () => {
    const rows = [
      { name: 'Ana', uf: 'SP' },
      { name: 'Bruno', uf: 'RJ' },
    ];
    const csv = toCSV(rows, [
      { key: 'name', header: 'nome' },
      { key: 'uf', header: 'uf' },
    ]);
    expect(csv).toBe('nome,uf\nAna,SP\nBruno,RJ');
  });
});
