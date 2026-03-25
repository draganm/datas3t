import type { Datas3tInfo, ListDatarangesResponse } from "./types";

export async function fetchDatas3ts(): Promise<Datas3tInfo[]> {
  const res = await fetch("/api/v1/datas3ts");
  if (!res.ok) throw new Error(`Failed to fetch datas3ts: ${res.statusText}`);
  return res.json();
}

export async function fetchDataranges(
  datas3tName: string,
): Promise<ListDatarangesResponse> {
  const res = await fetch(
    `/api/v1/dataranges?datas3t_name=${encodeURIComponent(datas3tName)}`,
  );
  if (!res.ok)
    throw new Error(`Failed to fetch dataranges: ${res.statusText}`);
  return res.json();
}
