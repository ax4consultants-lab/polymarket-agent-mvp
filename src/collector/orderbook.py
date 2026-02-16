"""CLOB orderbook fetching using py-clob-client."""
import time
from typing import Optional, List
from py_clob_client.client import ClobClient
import logging

from src.core.types import OrderBook, OrderBookLevel


logger = logging.getLogger("polymarket_bot.orderbook")

# Initialize read-only CLOB client (no auth needed)
clob_client = ClobClient(host="https://clob.polymarket.com")

def calculate_depth_within_1pct(bids: List, asks: List, mid_price: float) -> float:
    """
    Calculate notional depth within 1% of mid price.
    
    Returns total USD value of liquidity within ±1% of mid.
    """
    if mid_price <= 0:
        return 0.0
    
    bid_depth = 0.0
    ask_depth = 0.0
    
    # Bids within 1% below mid
    threshold_bid = mid_price * 0.99
    for bid in bids:
        if bid.price >= threshold_bid:
            bid_depth += bid.price * bid.size
    
    # Asks within 1% above mid
    threshold_ask = mid_price * 1.01
    for ask in asks:
        if ask.price <= threshold_ask:
            ask_depth += ask.price * ask.size
    
    return bid_depth + ask_depth

def fetch_orderbook(token_id: str, timeout: float = 2.0) -> Optional[OrderBook]:
    """
    Fetch orderbook for a single token from CLOB API.
    
    Returns None if fetch fails (timeout, API error, etc).
    """
    try:
        raw_book = clob_client.get_order_book(token_id)
        
        if not raw_book:
            return None
        
        # Parse bids (OrderSummary objects with .price and .size)
        bids = []
        for bid in raw_book.bids:
            bids.append(OrderBookLevel(
                price=float(bid.price),
                size=float(bid.size)
            ))
        
        # Parse asks
        asks = []
        for ask in raw_book.asks:
            asks.append(OrderBookLevel(
                price=float(ask.price),
                size=float(ask.size)
            ))
        
        # Skip if no liquidity
        if not bids or not asks:
            return None
        
        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None

        # --- Orderbook validity classification ---
        invalid_reason = None

        if best_bid is None:
            invalid_reason = "no_bid"
        elif best_ask is None:
            invalid_reason = "no_ask"
        elif best_bid < 0 or best_ask > 1:
            invalid_reason = "out_of_range"
        elif best_ask <= best_bid:
            invalid_reason = "crossed_or_locked"
        elif not (float("-inf") < best_bid < float("inf")) or not (float("-inf") < best_ask < float("inf")):
            invalid_reason = "nan_or_inf"

        if invalid_reason is not None:
            mid_price = 0.0
            spread_bps = None
            depth_within_1pct = 0.0
        else:
            mid_price = (best_bid + best_ask) / 2.0
            if mid_price <= 0:
                invalid_reason = "invalid_mid"
                mid_price = 0.0
                spread_bps = None
                depth_within_1pct = 0.0
            else:
                spread_bps = ((best_ask - best_bid) / mid_price) * 10_000.0
                depth_within_1pct = calculate_depth_within_1pct(bids, asks, mid_price)


        return OrderBook(
            market_id="",  # Will be set by caller
            token_id=token_id,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            spread_bps=spread_bps,
            last_trade_price=None, # Not provided by CLob API 120226
            depth_within_1pct=depth_within_1pct, #added 120226
            timestamp=time.time()
        )
        
    except Exception as e:
        # Log specific error types for observability
        logger.warning(
            f"Failed to fetch orderbook for token {token_id[:20]}...: "
            f"{type(e).__name__}: {str(e)}"
        )
        return None


def fetch_orderbooks_for_markets(
    markets: List, 
    max_tokens: int = 200,
    timeout_per_token: float = 2.0,
    rate_limit_delay: float = 0.05
) -> List[OrderBook]:
    """
    Fetch orderbooks for tokens in discovered markets.
    
    Args:
        markets: List of Market objects
        max_tokens: Maximum number of tokens to fetch (prevents hanging)
        timeout_per_token: Timeout per API call
        rate_limit_delay: Delay between requests (50ms default)
    
    Returns:
        List of OrderBook objects (only successful fetches)
    """
    orderbooks = []
    tokens_processed = 0
    
    for market in markets:
        for token in market.tokens:
            if tokens_processed >= max_tokens:
                logger.warning(f"Hit max_tokens limit ({max_tokens}), stopping orderbook fetch")
                return orderbooks
            
            book = fetch_orderbook(token.id, timeout=timeout_per_token)
            if book:
                book.market_id = market.id  # Associate with market
                orderbooks.append(book)
            
            tokens_processed += 1
            
            # Progress log every 50 tokens
            if tokens_processed % 50 == 0:
                logger.info(f"Fetched {len(orderbooks)}/{tokens_processed} orderbooks...")
            
            # Rate limit to avoid overwhelming API
            time.sleep(rate_limit_delay)
    
    return orderbooks
