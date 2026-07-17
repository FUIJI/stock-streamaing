import pytest
from pydantic import ValidationError

from src.schemas import Trade


def test_trade_json_round_trip():
    trade = Trade(symbol="AAPL", price=189.5, volume=100.0, ts=1700000000000)
    restored = Trade.model_validate_json(trade.model_dump_json())
    assert restored == trade


@pytest.mark.parametrize(
    "kwargs",
    [
        {"symbol": "", "price": 10.0, "volume": 1.0, "ts": 1},
        {"symbol": "AAPL", "price": 0.0, "volume": 1.0, "ts": 1},
        {"symbol": "AAPL", "price": -5.0, "volume": 1.0, "ts": 1},
        {"symbol": "AAPL", "price": 10.0, "volume": -1.0, "ts": 1},
        {"symbol": "AAPL", "price": 10.0, "volume": 1.0, "ts": 0},
    ],
)
def test_trade_rejects_invalid(kwargs):
    with pytest.raises(ValidationError):
        Trade(**kwargs)
