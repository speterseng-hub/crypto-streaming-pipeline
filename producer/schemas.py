"""
Data models for trade messages.
Using Pydantic for validation at the system boundary (WebSocket input).
"""
from pydantic import BaseModel, field_validator
import json


class CoinbaseTrade(BaseModel):
    """Raw trade event from Coinbase WebSocket."""
    trade_id: str
    product_id: str          # e.g. "BTC-USD"
    price: float
    size: float              # volume of the trade
    side: str                # "BUY" or "SELL"
    timestamp: str           # ISO 8601

    @field_validator("side")
    @classmethod
    def side_must_be_valid(cls, v: str) -> str:
        if v.upper() not in {"BUY", "SELL"}:
            raise ValueError(f"Invalid side: {v}")
        return v.upper()

    def to_pubsub_bytes(self) -> bytes:
        """Serialize to JSON bytes for Pub/Sub."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_pubsub_bytes(cls, data: bytes) -> "CoinbaseTrade":
        return cls.model_validate_json(data)


class PubSubMessage(BaseModel):
    """Pub/Sub message envelope with attributes."""
    data: CoinbaseTrade
    attributes: dict = {}
