from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class Position(BaseModel):
    """Running position for one symbol, recomputed from the fill stream."""

    symbol: str
    quantity: Decimal
    avg_price: Decimal
    last_updated: datetime
    last_fill_id: str | None = None
