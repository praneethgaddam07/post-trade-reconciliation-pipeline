from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from posttrade.models.enums import Exchange, Side


class Tick(BaseModel):
    """A single normalized market data update for one symbol.

    `sequence` is assigned by our ingestor per symbol (not the exchange's own
    sequence number) so that gaps introduced anywhere downstream — dropped
    Redis messages, a crashed worker, a lost DB write — are detectable later
    by the reconciler without needing exchange-side sequence data.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    sequence: int
    price: Decimal
    size: Decimal | None = None
    side: Side | None = None
    exchange: Exchange
    channel: str
    exchange_timestamp: datetime
    ingest_timestamp: datetime
