"""Centralized reason codes for orderbook validity and signal filters."""
from enum import Enum


class BookValidityReason(Enum):
    """Orderbook invalidity reasons."""
    NO_BID = "no_bid"
    NO_ASK = "no_ask"
    OUT_OF_RANGE = "out_of_range"
    CROSSED_OR_LOCKED = "crossed_or_locked"
    NAN_OR_INF = "nan_or_inf"
    INVALID_MID = "invalid_mid"


class FilterReason(Enum):
    """Signal filter rejection reasons."""
    SPREAD_TOO_WIDE = "spread_too_wide"
    DEPTH_TOO_THIN = "depth_too_thin"
    SNAPSHOT_STALE = "snapshot_stale"
    INVALID_BOOK = "invalid_book"  # Book was already invalid
    ZERO_EXEC_PRICE = "zero_exec_price"  # Added for subtask 2
