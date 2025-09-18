export class HttpError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly payload?: unknown
  ) {
    super(message);
  }
}

export async function fetchJson<T>(
  url: string,
  init?: RequestInit
): Promise<T> {
  const res = await fetch(url, init);
  const text = await res.text();

  if (!res.ok) {
    throw new HttpError(
      `Request failed: ${res.status} ${res.statusText}`,
      res.status,
      safeJsonParse(text)
    );
  }

  return (safeJsonParse(text) as T) ?? ({} as T);
}

function safeJsonParse(value: string): unknown {
  try {
    return value ? JSON.parse(value) : null;
  } catch {
    return value;
  }
}

