import type { PontoRow } from './PontosEditor';

export function parsePontosCSV(text: string): PontoRow[] {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length === 0) return [];

  const [rawHeader, ...rest] = lines;
  const header = (rawHeader ?? '').toLowerCase().split(/[,;\t]/).map((s) => s.trim());
  const hasHeader = header.includes('lat') && header.includes('lon');
  const dataLines = hasHeader ? rest : lines;

  const latIndex = hasHeader ? header.indexOf('lat') : 0;
  const lonIndex = hasHeader ? header.indexOf('lon') : 1;
  const idIndex = hasHeader ? header.indexOf('identificador') : 2;

  return dataLines.map((line) => {
    const cells = line.split(/[,;\t]/).map((cell) => cell.trim());
    return {
      lat: cells[latIndex] ?? '',
      lon: cells[lonIndex] ?? '',
      identificador: idIndex >= 0 ? (cells[idIndex] ?? '') : '',
    };
  });
}
