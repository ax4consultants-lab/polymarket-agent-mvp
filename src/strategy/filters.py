from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from src.core.types import OrderBook
from src.core.config import SignalsConfig


@dataclass
class FilterResult:
    accepted: bool
    reason: Optional[str] = None  # None if passed all filters


class SignalFilters:
    """
    Deterministic filter pipeline for candidate markets.

    Order:
    1) Spread filter
    2) Depth filter
    3) Staleness filter (stub – ready for later wiring)
    """

    def __init__(self, cfg: SignalsConfig) -> None:
        self.cfg = cfg

    def apply(self, book: OrderBook, snapshot_age_s: Optional[float] = None) -> FilterResult:
        """
        Apply all filters in order. First failure wins and returns its reason code.
        """
        ok, reason = self._spread_filter(book)
        if not ok:
            return FilterResult(accepted=False, reason=reason)

        ok, reason = self._depth_filter(book)
        if not ok:
            return FilterResult(accepted=False, reason=reason)

        ok, reason = self._staleness_filter(snapshot_age_s)
        if not ok:
            return FilterResult(accepted=False, reason=reason)

        return FilterResult(accepted=True, reason=None)

    # --- individual filters ---

    def _spread_filter(self, book: OrderBook) -> Tuple[bool, Optional[str]]:
        spread_bps = book.spread_bps if book.spread_bps is not None else 99999.0
        if spread_bps > self.cfg.max_spread_bps:
            return False, "spread_too_wide"
        return True, None

    def _depth_filter(self, book: OrderBook) -> Tuple[bool, Optional[str]]:
        depth_usdc = book.depth_within_1pct or 0.0
        if depth_usdc < self.cfg.min_depth_usdc:
            return False, "depth_too_thin"
        return True, None

    def _staleness_filter(self, snapshot_age_s: Optional[float]) -> Tuple[bool, Optional[str]]:
        """
        For now, treat None as fresh; once runner passes real age, enforce max_snapshot_age_s.
        """
        if snapshot_age_s is None:
            return True, None

        if snapshot_age_s > self.cfg.max_snapshot_age_s:
            return False, "snapshot_stale"

        return True, None
