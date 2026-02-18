from __future__ import annotations

from dataclasses import dataclass
from typing import List
import math

from src.core.types import OrderBook
from src.core.config import SignalsConfig
from src.strategy.fair_value import FairValueEstimator
from src.strategy.filters import SignalFilters, FilterResult


@dataclass
class Signal:
    cycle_id: int
    market_id: str
    token_id: str
    side: str  # "buy" or "sell"
    fair_value_prob: float
    p_implied_mid: float
    p_implied_exec_buy: float
    p_implied_exec_sell: float
    edge_bps: float
    filter_reason: str | None
    spread_bps: float
    depth_within_1pct: float | None


class SignalGenerator:
    """
    Deterministic signal generator:
    - derive implied probabilities from book
    - compare to fair value
    - apply filters
    """

    def __init__(self, cfg: SignalsConfig) -> None:
        self.cfg = cfg
        self.fair_value = FairValueEstimator(cfg)
        self.filters = SignalFilters(cfg)

    def _implied_probs(self, book: OrderBook) -> tuple[float, float, float]:
        """
        Map prices to probabilities in [0,1].

        For a YES token:
        - mid prob: mid_price
        - exec buy prob: best_ask
        - exec sell prob: best_bid
        """
        mid = book.mid_price or 0.0
        best_bid = book.best_bid or 0.0
        best_ask = book.best_ask or 0.0

        p_mid = max(0.0, min(1.0, mid))
        p_exec_buy = max(0.0, min(1.0, best_ask))
        p_exec_sell = max(0.0, min(1.0, best_bid))

        return p_mid, p_exec_buy, p_exec_sell

    def generate_signals(self, cycle_id: int, books: List[OrderBook]) -> List[Signal]:
        """
        Generate candidate signals for a list of orderbooks.

        For MVP:
        - compute fair value vs mid prob
        - edge_bps = (fair_value - p_implied_mid) * 10_000
        - we emit both buy and sell directions symmetrically
        """
        signals: List[Signal] = []

        # Deterministic order: sort by market_id, token_id
        books_sorted = sorted(books, key=lambda b: (b.market_id, b.token_id))

        for book in books_sorted:
            p_mid, p_exec_buy, p_exec_sell = self._implied_probs(book)
            fv = self.fair_value.fair_value_prob(book)

            # Positive edge: want to buy if fair > market
            edge_buy_bps = (fv - p_exec_buy) * 10_000
            # Negative edge: want to sell if fair < market
            edge_sell_bps = (p_exec_sell - fv) * 10_000

            # Apply filters once per book (for now, same for both sides)
            # TODO: pass real snapshot_age_s from runner
            filt: FilterResult = self.filters.apply(book, snapshot_age_s=None)

            # BUY signal - guard against zero/non-finite exec price
            if p-exec_buy > 0 and math.isfinite(p_exec_buy):
                signals.append(
                    Signal(
                        cycle_id=cycle_id,
                        market_id=book.market_id,
                        token_id=book.token_id,
                        side="buy",
                        fair_value_prob=fv,
                        p_implied_mid=p_mid,
                        p_implied_exec_buy=p_exec_buy,
                        p_implied_exec_sell=p_exec_sell,
                        edge_bps=edge_buy_bps,
                        filter_reason=filt.reason,
                        spread_bps=book.spread_bps or 0.0,
                        depth_within_1pct=book.depth_within_1pct,
                    )
                )

        # SELL signal - guard against zero/non-finite exec price
        if p_exec_sell > 0 and math.isfinite(p_exec_sell):
            signals.append(
                Signal(
                    cycle_id=cycle_id,
                    market_id=book.market_id,
                    token_id=book.token_id,
                    side="sell",
                    fair_value_prob=fv,
                    p_implied_mid=p_mid,
                    p_implied_exec_buy=p_exec_buy,
                    p_implied_exec_sell=p_exec_sell,
                    edge_bps=edge_sell_bps,
                    filter_reason=filt.reason,
                    spread_bps=book.spread_bps or 0.0,
                    depth_within_1pct=book.depth_within_1pct,
                )
            )

        # Sort by absolute edge descending, keep top N for logging
        signals.sort(key=lambda s: abs(s.edge_bps), reverse=True)

        if len(signals) > self.cfg.top_n_to_log:
            return signals[: self.cfg.top_n_to_log]

        return signals
