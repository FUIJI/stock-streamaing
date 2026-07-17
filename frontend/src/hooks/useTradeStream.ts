"use client";

import { useEffect, useRef } from "react";

import { API_URL } from "@/lib/api";

export interface Trade {
  symbol: string;
  price: number;
  volume: number;
  ts: number;
}

const WS_BASE = (process.env.NEXT_PUBLIC_WS_URL ?? API_URL).replace(/^http/, "ws");

/** Subscribes to live trades for a symbol; reconnects automatically. */
export function useTradeStream(symbol: string, onTrade: (trade: Trade) => void) {
  const handlerRef = useRef(onTrade);
  useEffect(() => {
    handlerRef.current = onTrade;
  });

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let disposed = false;

    const connect = () => {
      ws = new WebSocket(`${WS_BASE}/ws/${encodeURIComponent(symbol)}`);
      ws.onmessage = (event) => handlerRef.current(JSON.parse(event.data));
      ws.onclose = () => {
        if (!disposed) retryTimer = setTimeout(connect, 2000);
      };
    };
    connect();

    return () => {
      disposed = true;
      clearTimeout(retryTimer);
      ws?.close();
    };
  }, [symbol]);
}
