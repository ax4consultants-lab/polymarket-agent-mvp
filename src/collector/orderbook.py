"""CLOB orderbook fetching using py-clob-client."""
import time
from typing import Optional, List
from py_clob_client.client import ClobClient
import logging

from src.core.types import OrderBook, OrderBookLevel


logger = logging.getLogger("polymarket_bot.orderbook")

# Initialize read-only CLOB client (no auth needed)
clob_client = ClobClient(host="https://clob.polymarket.com")


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
        
        # Compute metrics
        best_bid = bids[0].price if bids else 0.0
        best_ask = asks[0].price if asks else 0.0
        mid_price = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0
        spread_bps = ((best_ask - best_bid) / best_bid * 10000) if best_bid > 0 else 9999.0
        
        return OrderBook(
            market_id="",  # Will be set by caller
            token_id=token_id,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            spread_bps=spread_bps,
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
