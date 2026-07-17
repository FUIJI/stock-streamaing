from pydantic import BaseModel, Field, PositiveFloat


class Trade(BaseModel):
    """Normalized trade message published to the trades topic (JSON, keyed by symbol)."""

    symbol: str = Field(min_length=1)
    price: PositiveFloat
    volume: float = Field(ge=0)
    ts: int = Field(gt=0, description="Event time, epoch milliseconds")
