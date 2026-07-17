import pytest

from src.sources import SimulatorSource, build_source


async def take(source, n: int):
    trades = []
    async for trade in source.stream():
        trades.append(trade)
        if len(trades) >= n:
            break
    return trades


async def test_simulator_produces_valid_trades():
    symbols = ["AAPL", "MSFT"]
    source = SimulatorSource(symbols, trades_per_sec=1000.0, seed=42)
    trades = await take(source, 50)

    assert {t.symbol for t in trades} == set(symbols)
    assert all(t.price > 0 for t in trades)
    assert all(t.volume >= 0 for t in trades)
    # Event time must never go backwards within the stream.
    ts_values = [t.ts for t in trades]
    assert ts_values == sorted(ts_values)


async def test_simulator_round_robin_covers_all_symbols():
    symbols = ["A", "B", "C"]
    source = SimulatorSource(symbols, trades_per_sec=1000.0, seed=1)
    trades = await take(source, 9)
    assert [t.symbol for t in trades] == symbols * 3


def test_simulator_requires_symbols():
    with pytest.raises(ValueError):
        SimulatorSource([], trades_per_sec=5.0)


def test_build_source_rejects_unknown():
    with pytest.raises(ValueError):
        build_source("carrier-pigeon", ["AAPL"], 5.0, "")


def test_build_source_finnhub_requires_key():
    with pytest.raises(ValueError):
        build_source("finnhub", ["AAPL"], 5.0, "")
