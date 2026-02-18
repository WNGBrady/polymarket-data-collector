const API_URL = process.env.NEXT_PUBLIC_API_URL || "";

export function buildUrl(
  endpoint: string,
  params: { game?: string; date_start?: string; date_end?: string; [key: string]: string | number | undefined }
) {
  const path = `${API_URL}/api/${endpoint}`;
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      searchParams.set(key, String(value));
    }
  }
  const qs = searchParams.toString();
  return qs ? `${path}?${qs}` : path;
}

export async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export { API_URL };
