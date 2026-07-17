export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export interface Candle {
  symbol: string;
  bucket_start: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  trade_count: number;
}

export async function fetchCandles(
  symbol: string,
  interval = "1m",
  limit = 300,
): Promise<Candle[]> {
  const params = new URLSearchParams({ symbol, interval, limit: String(limit) });
  const res = await fetch(`${API_URL}/candles?${params}`);
  if (!res.ok) throw new Error(`GET /candles failed: ${res.status}`);
  return res.json();
}

export async function fetchSymbols(): Promise<string[]> {
  const res = await fetch(`${API_URL}/symbols`);
  if (!res.ok) throw new Error(`GET /symbols failed: ${res.status}`);
  return res.json();
}
