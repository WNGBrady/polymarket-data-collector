import useSWR from "swr";
import { useFilters } from "@/context/FilterContext";
import { buildUrl, fetcher } from "./api";

const REFRESH_INTERVAL = 120_000; // 2 minutes

export function useApiData<T>(endpoint: string, extraParams: Record<string, string | number | undefined> = {}) {
  const { game, dateStart, dateEnd } = useFilters();

  const url = buildUrl(endpoint, {
    game,
    date_start: dateStart,
    date_end: dateEnd,
    ...extraParams,
  });

  return useSWR<T>(url, fetcher, {
    refreshInterval: REFRESH_INTERVAL,
    revalidateOnFocus: false,
    dedupingInterval: 30_000,
  });
}
