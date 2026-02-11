"""Market discovery using Polymarket Gamma API."""
import httpx
from typing import List, Optional

from src.core.types import Market, Token
from src.core.config import AppConfig


GAMMA_API_BASE = "https://gamma-api.polymarket.com"


def fetch_markets_from_gamma(
    limit: int = 100,
    offset: int = 0,
    active: bool = True,
    closed: bool = False,
    archived: bool = False
) -> List[dict]:
    """Fetch markets from Gamma API."""
    url = f"{GAMMA_API_BASE}/markets"
    params = {
        "limit": limit,
        "offset": offset,
        "active": str(active).lower(),
        "closed": str(closed).lower(),
        "archived": str(archived).lower(),
    }
    
    try:
        response = httpx.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []
    except httpx.HTTPError as e:
        raise RuntimeError(f"Failed to fetch markets from Gamma API: {e}")


def normalize_market(raw: dict) -> Optional[Market]:
    """Convert raw Gamma API response to Market object."""
    try:
        # Parse tokens
        tokens = []
        for outcome in raw.get("outcomes", []):
            token = Token(
                id=outcome.get("token_id", ""),
                symbol=outcome.get("symbol", ""),
                outcome=outcome.get("outcome", ""),
                decimals=6  # Standard for Polymarket
            )
            tokens.append(token)
        
        # Extract market fields
        market = Market(
            id=raw.get("id", ""),
            question=raw.get("question", ""),
            tokens=tokens,
            volume_24h=float(raw.get("volume24hr", 0.0)),
            liquidity=float(raw.get("liquidity", 0.0)),
            spread_bps=0.0,  # Will be computed from orderbook later
            active=raw.get("active", False)
        )
        return market
    except (KeyError, ValueError, TypeError) as e:
        # Skip malformed markets
        return None
def normalize_market(raw: dict) -> Optional[Market]:
    """Convert raw Gamma API response to Market object."""
    try:
        # Parse tokens - handle both dict and string outcomes
        tokens = []
        outcomes = raw.get("outcomes", [])
        outcome_prices = raw.get("outcomePrices", [])
        
        # outcomes might be ["Yes", "No"] as strings
        # or might be dicts with token_id field
        for i, outcome in enumerate(outcomes):
            if isinstance(outcome, dict):
                # Outcome is a dict with token_id, etc.
                token = Token(
                    id=outcome.get("token_id", ""),
                    symbol=outcome.get("symbol", "YES" if i == 0 else "NO"),
                    outcome=outcome.get("outcome", str(outcome)),
                    decimals=6
                )
            else:
                # Outcome is just a string like "Yes" or "No"
                # Token IDs should be in a separate field
                token_ids = raw.get("tokens", [])
                token_id = token_ids[i] if i < len(token_ids) else f"token_{i}"
                
                token = Token(
                    id=token_id,
                    symbol="YES" if i == 0 else "NO",
                    outcome=str(outcome),
                    decimals=6
                )
            tokens.append(token)
        
        # If no tokens parsed, skip this market
        if not tokens:
            return None
        
        # Extract market fields
        market = Market(
            id=raw.get("id", raw.get("condition_id", "")),
            question=raw.get("question", raw.get("title", "")),
            tokens=tokens,
            volume_24h=float(raw.get("volume24hr", raw.get("volume", 0.0))),
            liquidity=float(raw.get("liquidity", 0.0)),
            spread_bps=0.0,  # Will be computed from orderbook later
            active=raw.get("active", True)
        )
        return market
    except (KeyError, ValueError, TypeError, IndexError) as e:
        # Skip malformed markets
        return None


def apply_filters(market: Market, config: AppConfig) -> bool:
    """Apply config filters to market."""
    filters = config.market_filters
    
    # Volume filter
    if market.volume_24h < filters.min_volume_24h:
        return False
    
    # Keyword deny list
    question_lower = market.question.lower()
    for deny_word in filters.deny_keywords:
        if deny_word.lower() in question_lower:
            return False
    
    # Keyword allow list (if specified, at least one must match)
    if filters.allow_keywords:
        has_match = any(
            kw.lower() in question_lower
            for kw in filters.allow_keywords
        )
        if not has_match:
            return False
    
    return True


def discover_markets(config: AppConfig) -> List[Market]:
    """Discover and filter active markets."""
    raw_markets = fetch_markets_from_gamma(limit=100, active=True, closed=False)
    
    markets = []
    for raw in raw_markets:
        market = normalize_market(raw)
        if market and apply_filters(market, config):
            markets.append(market)
    
    return markets
