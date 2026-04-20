#!/usr/bin/env node
import { execSync } from 'node:child_process';

const url = process.env.VITE_API_BASE_URL
  ? `${process.env.VITE_API_BASE_URL.replace(/\/$/, '')}/openapi.json`
  : 'http://localhost:8000/openapi.json';

const output = 'src/api/schema.d.ts';

process.stdout.write(`Fetching OpenAPI schema from ${url}...\n`);

try {
  execSync(`npx -y openapi-typescript ${url} -o ${output}`, { stdio: 'inherit' });
  process.stdout.write(`\nTypes written to ${output}\n`);
} catch (err) {
  process.stderr.write(
    `\nFailed to generate types from ${url}.\n` +
      `Is the backend running? Try: uvicorn src.app:app --reload\n` +
      `Or set VITE_API_BASE_URL to the backend URL.\n`,
  );
  process.exit(1);
}
