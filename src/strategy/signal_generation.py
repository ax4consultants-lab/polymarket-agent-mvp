from dataclasses import dataclass

from src.core.types import OrderBook, Side


@dataclass(frozen=True)
class ImpliedPrices:
    p_implied_mid: float
    p_implied_exec_buy: float
    p_implied_exec_sell: float


@dataclass(frozen=True)
class SignalCandidate:
    market_id: str
    token_id: str
    side: Side
    fair_value: float
    edge_bps: float
    ranking_price: float
    implied_prices: ImpliedPrices


def compute_implied_prices(orderbook: OrderBook) -> ImpliedPrices:
    """Compute implied mid and executable prices from top-of-book quotes."""
    if orderbook.mid_price is None or orderbook.best_ask is None or orderbook.best_bid is None:
        raise ValueError("Orderbook must include mid_price, best_ask, and best_bid")

    return ImpliedPrices(
        p_implied_mid=orderbook.mid_price,
        p_implied_exec_buy=orderbook.best_ask,
        p_implied_exec_sell=orderbook.best_bid,
    )


def create_signal_candidate(orderbook: OrderBook, fair_value: float, side: Side) -> SignalCandidate:
    """Create a candidate using side-specific executable price for edge ranking."""
    implied = compute_implied_prices(orderbook)

    if side == "buy":
        ranking_price = implied.p_implied_exec_buy
        edge_bps = ((fair_value - ranking_price) / ranking_price) * 10_000
    else:
        ranking_price = implied.p_implied_exec_sell
        edge_bps = ((ranking_price - fair_value) / ranking_price) * 10_000

    return SignalCandidate(
        market_id=orderbook.market_id,
        token_id=orderbook.token_id,
        side=side,
        fair_value=fair_value,
        edge_bps=edge_bps,
        ranking_price=ranking_price,
        implied_prices=implied,
    )


def rank_candidates(candidates: list[SignalCandidate]) -> list[SignalCandidate]:
    """Rank candidates by edge descending."""
    return sorted(candidates, key=lambda c: c.edge_bps, reverse=True)
