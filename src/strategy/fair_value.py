from __future__ import annotations

from typing import Dict, Optional

from src.core.types import OrderBook
from src.core.config import SignalsConfig


class FairValueEstimator:
    """
    EMA-based fair value estimator over mid prices.

    State is in-memory per process; for MVP we do not persist EMA across restarts.
    """

    def __init__(self, cfg: SignalsConfig) -> None:
        self.cfg = cfg
        # key: token_id -> ema_mid
        self._ema_state: Dict[str, float] = {}

    def _update_ema(self, token_id: str, mid_price: float) -> float:
        """
        Update EMA for a token_id and return the new EMA.
        If no prior value, seed EMA with the current mid.
        """
        if mid_price <= 0:
            # Degenerate mid, do not update; return existing or 0.5
            if token_id in self._ema_state:
                return self._ema_state[token_id]
            return 0.5

        prev = self._ema_state.get(token_id)
        alpha = self.cfg.ema_alpha

        if prev is None:
            ema = mid_price
        else:
            ema = alpha * mid_price + (1 - alpha) * prev

        self._ema_state[token_id] = ema
        return ema

    def fair_value_prob(self, book: OrderBook) -> float:
        """
        Return a fair value probability in [0, 1].

        Priority:
        1) Valid mid_price -> EMA over mid
        2) If EMA not usable, fall back to mid directly
        3) If no mid (e.g. one side missing), fall back to 0.5
        """
        mid = book.mid_price or 0.0

        if mid > 0:
            ema_mid = self._update_ema(book.token_id, mid)
            return max(0.0, min(1.0, ema_mid))

        # No good mid; use simple fallback
        return 0.5
