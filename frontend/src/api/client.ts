import type { components } from './schema';

export type ProblemDetails = components['schemas']['ProblemDetails'];

export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | undefined;

  constructor(message: string, status: number, problem?: ProblemDetails) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.problem = problem;
  }

  get title(): string {
    return this.problem?.title ?? this.message;
  }

  get detail(): string | null | undefined {
    return this.problem?.detail;
  }
}

const baseUrl = (import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000').replace(/\/$/, '');

export type QueryParams = Readonly<Record<string, unknown>> | object;

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  query?: QueryParams | undefined;
  body?: unknown;
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

function buildUrl(path: string, query: QueryParams | undefined): string {
  const url = new URL(`${baseUrl}${path}`);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === '') continue;
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item !== undefined && item !== null && item !== '') {
            url.searchParams.append(key, String(item));
          }
        }
      } else {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url.toString();
}

function isFormData(value: unknown): value is FormData {
  return typeof FormData !== 'undefined' && value instanceof FormData;
}

async function parseProblem(response: Response): Promise<ProblemDetails | undefined> {
  const contentType = response.headers.get('content-type') ?? '';
  if (!contentType.includes('json')) return undefined;
  try {
    return (await response.json()) as ProblemDetails;
  } catch {
    return undefined;
  }
}

export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { method = 'GET', query, body, signal, headers: extraHeaders } = options;

  const url = buildUrl(path, query);
  const headers: Record<string, string> = { Accept: 'application/json', ...extraHeaders };

  let requestBody: BodyInit | undefined;
  if (body !== undefined && body !== null) {
    if (isFormData(body)) {
      requestBody = body;
    } else {
      headers['Content-Type'] = 'application/json';
      requestBody = JSON.stringify(body);
    }
  }

  const response = await fetch(url, {
    method,
    headers,
    body: requestBody,
    signal,
  });

  if (!response.ok) {
    const problem = await parseProblem(response);
    const message = problem?.title ?? `${response.status} ${response.statusText}`;
    throw new ApiError(message, response.status, problem);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get('content-type') ?? '';
  if (!contentType.includes('json')) {
    return (await response.text()) as unknown as T;
  }
  return (await response.json()) as T;
}
