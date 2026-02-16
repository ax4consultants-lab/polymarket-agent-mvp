from typing import List, Literal, Optional
from pydantic import BaseModel


Side = Literal["buy", "sell"]


class Token(BaseModel):
    id: str
    symbol: str
    outcome: str
    decimals: int


class Market(BaseModel):
    id: str
    question: str
    tokens: List[Token]
    volume_24h: float
    liquidity: float
    spread_bps: float
    active: bool


class OrderBookLevel(BaseModel):
    price: float
    size: float


class OrderBook(BaseModel):
    market_id: str
    token_id: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid_price: Optional[float] = None
    spread_bps: Optional[float] = None
    last_trade_price: Optional[float] = None
    depth_within_1pct: Optional[float] = None
    timestamp: float
    validity_reason: Optional[str] = None  # NEW: None means valid, otherwise reason code


class Estimate(BaseModel):
    market_id: str
    token_id: str
    side: Side
    fair_value: float
    market_price: float
    gross_edge_bps: float
    fee_est_bps: float
    slippage_est_bps: float
    net_edge_bps: float
    confidence: Optional[float] = None


class Decision(BaseModel):
    id: Optional[int] = None
    cycle_id: Optional[int] = None
    estimate_id: Optional[int] = None
    market_id: str
    token_id: str
    side: Side
    decision: Literal["trade", "skip"]
    reason: str
    kelly_fraction: Optional[float] = None
    target_size: Optional[float] = None
    target_price: Optional[float] = None


class AccountState(BaseModel):
    equity: float
    cash: float
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    total_exposure: float = 0.0


class PaperFill(BaseModel):
    market_id: str
    token_id: str
    side: Side
    size: float
    avg_fill_price: float
    total_cost: float
    slippage_bps: float
    fees_paid: float
    timestamp: float
