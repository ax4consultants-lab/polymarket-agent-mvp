"""Market discovery using Polymarket Gamma API."""
import httpx
import json
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
        # Parse outcomes and token IDs (both are JSON strings)
        outcomes = json.loads(raw.get("outcomes", "[]"))
        clob_token_ids = json.loads(raw.get("clobTokenIds", "[]"))
        
        # Build tokens
        tokens = []
        for i, outcome in enumerate(outcomes):
            token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
            
            if not token_id:
                continue  # Skip if no valid token ID
            
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
            id=raw.get("id", ""),
            question=raw.get("question", ""),
            tokens=tokens,
            volume_24h=float(raw.get("volume24hr", 0.0)),
            liquidity=float(raw.get("liquidityNum", raw.get("liquidity", 0.0))),
            spread_bps=0.0,  # Will be computed from orderbook later
            active=raw.get("active", True)
        )
        return market
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
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
