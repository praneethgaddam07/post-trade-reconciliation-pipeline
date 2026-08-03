from datetime import datetime

from pydantic import BaseModel, ConfigDict

from posttrade.models.enums import BreakType


class Break(BaseModel):
    """A reconciliation break flagged between the simulated book and observed market data."""

    model_config = ConfigDict(frozen=True)

    break_type: BreakType
    symbol: str
    detected_at: datetime
    detail: str
    related_fill_id: str | None = None
    related_sequence: int | None = None
