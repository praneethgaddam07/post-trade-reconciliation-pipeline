from enum import StrEnum


class Exchange(StrEnum):
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    SIMULATED = "simulated"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class BreakType(StrEnum):
    UNMATCHED_FILL = "unmatched_fill"
    POSITION_DRIFT = "position_drift"
    SEQUENCE_GAP = "sequence_gap"
