import { describe, expect, it } from 'vitest';
import { parsePontosCSV } from './pontosCsv';

describe('parsePontosCSV', () => {
  it('parses CSV with lat,lon,identificador header', () => {
    const out = parsePontosCSV('lat,lon,identificador\n-23.55,-46.63,forn-001');
    expect(out).toEqual([{ lat: '-23.55', lon: '-46.63', identificador: 'forn-001' }]);
  });

  it('parses semicolon-separated and header-less inputs', () => {
    const out = parsePontosCSV('-23.55;-46.63;forn-001\n10.0;20.0;x');
    expect(out).toEqual([
      { lat: '-23.55', lon: '-46.63', identificador: 'forn-001' },
      { lat: '10.0', lon: '20.0', identificador: 'x' },
    ]);
  });

  it('returns empty array for empty input', () => {
    expect(parsePontosCSV('')).toEqual([]);
    expect(parsePontosCSV('   \n\n')).toEqual([]);
  });

  it('defaults identificador to empty string when column is missing', () => {
    const out = parsePontosCSV('lat,lon\n-23.55,-46.63');
    expect(out).toEqual([{ lat: '-23.55', lon: '-46.63', identificador: '' }]);
  });
});
