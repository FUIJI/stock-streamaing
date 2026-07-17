"use client";

import {
  CandlestickData,
  CandlestickSeries,
  ISeriesApi,
  UTCTimestamp,
  createChart,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";

import { Trade, useTradeStream } from "@/hooks/useTradeStream";
import { fetchCandles } from "@/lib/api";

type LiveCandle = CandlestickData<UTCTimestamp>;

const UP = "#26a69a";
const DOWN = "#ef5350";

export default function CandlestickChart({ symbol }: { symbol: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const lastCandleRef = useRef<LiveCandle | null>(null);
  const [lastPrice, setLastPrice] = useState<number | null>(null);
  const [direction, setDirection] = useState<"up" | "down">("up");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "transparent" },
        textColor: "#9ca3af",
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.08)" },
        horzLines: { color: "rgba(148, 163, 184, 0.08)" },
      },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#1f2937" },
      rightPriceScale: { borderColor: "#1f2937" },
    });
    const series = chart.addSeries(CandlestickSeries, {
      upColor: UP,
      downColor: DOWN,
      wickUpColor: UP,
      wickDownColor: DOWN,
      borderVisible: false,
    });
    seriesRef.current = series;
    lastCandleRef.current = null;
    setLastPrice(null);
    setError(null);

    let cancelled = false;
    fetchCandles(symbol, "1m", 300)
      .then((candles) => {
        if (cancelled) return;
        const data: LiveCandle[] = candles.map((c) => ({
          time: (Date.parse(c.bucket_start) / 1000) as UTCTimestamp,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }));
        series.setData(data);
        lastCandleRef.current = data[data.length - 1] ?? null;
        if (lastCandleRef.current) setLastPrice(lastCandleRef.current.close);
        chart.timeScale().fitContent();
      })
      .catch(() => {
        if (!cancelled) setError("โหลดข้อมูลย้อนหลังไม่สำเร็จ — ตรวจว่า API รันอยู่ที่พอร์ต 8000");
      });

    return () => {
      cancelled = true;
      seriesRef.current = null;
      chart.remove();
    };
  }, [symbol]);

  useTradeStream(symbol, (trade: Trade) => {
    const series = seriesRef.current;
    if (!series) return;

    const bucket = (Math.floor(trade.ts / 60_000) * 60) as UTCTimestamp;
    const last = lastCandleRef.current;
    let candle: LiveCandle;
    if (!last || bucket > last.time) {
      candle = { time: bucket, open: trade.price, high: trade.price, low: trade.price, close: trade.price };
    } else if (bucket < last.time) {
      return; // stale trade from before the current candle
    } else {
      candle = {
        ...last,
        high: Math.max(last.high, trade.price),
        low: Math.min(last.low, trade.price),
        close: trade.price,
      };
    }
    lastCandleRef.current = candle;
    series.update(candle);
    setDirection((prev) => (lastPriceRefCompare(candle, trade.price, prev)));
    setLastPrice(trade.price);
  });

  return (
    <section className="chart-card">
      <header className="chart-header">
        <h2>{symbol}</h2>
        {lastPrice !== null && (
          <span className={`price price-${direction}`}>{lastPrice.toFixed(2)}</span>
        )}
        <span className="live-dot" title="live" />
      </header>
      {error && <p className="chart-error">{error}</p>}
      <div ref={containerRef} className="chart-container" />
    </section>
  );
}

function lastPriceRefCompare(candle: LiveCandle, price: number, prev: "up" | "down") {
  if (price > candle.open) return "up";
  if (price < candle.open) return "down";
  return prev;
}
