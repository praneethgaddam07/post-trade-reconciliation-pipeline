from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from posttrade.models.enums import Exchange, Side


class Trade(BaseModel):
    """An observed market trade (execution) reported by the exchange feed,
    as distinct from a `Fill` — which is our own simulated strategy's execution.
    """

    model_config = ConfigDict(frozen=True)

    trade_id: str
    symbol: str
    price: Decimal
    size: Decimal
    side: Side
    exchange: Exchange
    exchange_timestamp: datetime
