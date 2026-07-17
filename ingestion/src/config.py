import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    trade_source: str
    kafka_broker: str
    trades_topic: str
    symbols: list[str] = field(default_factory=list)
    sim_trades_per_sec: float = 5.0
    finnhub_api_key: str = ""

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            trade_source=os.environ.get("TRADE_SOURCE", "simulator").lower(),
            kafka_broker=os.environ.get("KAFKA_BROKER", "redpanda:9092"),
            trades_topic=os.environ.get("TRADES_TOPIC", "trades.raw"),
            symbols=[
                s.strip().upper()
                for s in os.environ.get("SYMBOLS", "AAPL,MSFT,NVDA,TSLA").split(",")
                if s.strip()
            ],
            sim_trades_per_sec=float(os.environ.get("SIM_TRADES_PER_SEC", "5")),
            finnhub_api_key=os.environ.get("FINNHUB_API_KEY", ""),
        )
