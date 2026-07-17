"use client";

import { useEffect, useState } from "react";

import CandlestickChart from "@/components/CandlestickChart";
import SymbolSelector from "@/components/SymbolSelector";
import { fetchSymbols } from "@/lib/api";

const DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA"];

export default function Home() {
  const [symbols, setSymbols] = useState<string[]>(DEFAULT_SYMBOLS);
  const [symbol, setSymbol] = useState(DEFAULT_SYMBOLS[0]);

  useEffect(() => {
    fetchSymbols()
      .then((available) => {
        if (available.length > 0) {
          setSymbols(available);
          setSymbol((current) => (available.includes(current) ? current : available[0]));
        }
      })
      .catch(() => {
        // API not up yet — keep defaults, chart shows its own error state.
      });
  }, []);

  return (
    <main className="dashboard">
      <header className="dashboard-header">
        <h1>Stock Streaming Platform</h1>
        <p>Real-time OHLCV · Redpanda → Spark → TimescaleDB → FastAPI</p>
      </header>
      <SymbolSelector symbols={symbols} value={symbol} onChange={setSymbol} />
      <CandlestickChart symbol={symbol} />
    </main>
  );
}
