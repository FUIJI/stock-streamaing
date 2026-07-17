"use client";

export default function SymbolSelector({
  symbols,
  value,
  onChange,
}: {
  symbols: string[];
  value: string;
  onChange: (symbol: string) => void;
}) {
  return (
    <nav className="symbol-selector">
      {symbols.map((symbol) => (
        <button
          key={symbol}
          type="button"
          data-active={symbol === value || undefined}
          onClick={() => onChange(symbol)}
        >
          {symbol}
        </button>
      ))}
    </nav>
  );
}
