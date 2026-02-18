const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export function buildUrl(
  endpoint: string,
  params: { game?: string; date_start?: string; date_end?: string; [key: string]: string | number | undefined }
) {
  const url = new URL(`${API_URL}/api/${endpoint}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

export async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export { API_URL };
