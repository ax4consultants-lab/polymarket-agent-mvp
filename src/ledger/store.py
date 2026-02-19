import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

class Store:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_cycle(self, timestamp: float, status: str, **kwargs) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO cycles (timestamp, status, markets_scanned,
               opportunities_found, decisions_made, error_message, execution_time_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                timestamp,
                status,
                kwargs.get("markets_scanned", 0),
                kwargs.get("opportunities_found", 0),
                kwargs.get("decisions_made", 0),
                kwargs.get("error_message"),
                kwargs.get("execution_time_ms"),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid


    def update_cycle(self, cycle_id: int, **kwargs) -> None:
        fields = []
        values = []
        for key, value in kwargs.items():
            fields.append(f"{key} = ?")
            values.append(value)
        values.append(cycle_id)
        sql = f"UPDATE cycles SET {', '.join(fields)} WHERE cycle_id = ?"
        self._conn.execute(sql, values)
        self._conn.commit()

    def record_account_state(self, cycle_id: int, equity: float, cash: float, **kwargs) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO account_states (cycle_id, timestamp, equity, cash,
               unrealized_pnl, realized_pnl, total_exposure)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                datetime.now().timestamp(),
                equity,
                cash,
                kwargs.get("unrealized_pnl"),
                kwargs.get("realized_pnl"),
                kwargs.get("total_exposure"),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_latest_account_state(self) -> Optional[Dict[str, Any]]:
        cursor = self._conn.execute(
            "SELECT * FROM account_states ORDER BY timestamp DESC LIMIT 1"
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def record_estimate(
        self,
        cycle_id: int,
        market_id: str,
        token_id: str,
        side: str,
        fair_value: float,
        market_price: float,
        gross_edge_bps: float,
        fee_est_bps: float,
        slippage_est_bps: float,
        net_edge_bps: float,
        confidence: Optional[float] = None,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO estimates (cycle_id, market_id, token_id, side,
               fair_value, market_price, gross_edge_bps, fee_est_bps,
               slippage_est_bps, net_edge_bps, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                market_id,
                token_id,
                side,
                fair_value,
                market_price,
                gross_edge_bps,
                fee_est_bps,
                slippage_est_bps,
                net_edge_bps,
                confidence,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid


    def record_decision(
        self,
        cycle_id: int,
        estimate_id: int,
        decision: str,
        reason: str,
        kelly_fraction: Optional[float] = None,
        target_size: Optional[float] = None,
        target_price: Optional[float] = None,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO decisions (cycle_id, estimate_id, decision, reason,
               kelly_fraction, target_size, target_price)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                estimate_id,
                decision,
                reason,
                kelly_fraction,
                target_size,
                target_price,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def record_paper_fill(
        self,
        cycle_id: int,
        decision_id: int,
        market_id: str,
        token_id: str,
        side: str,
        size: float,
        avg_fill_price: float,
        total_cost: float,
        slippage_bps: float,
        fees_paid: float,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO paper_fills (cycle_id, decision_id, market_id, token_id,
               side, size, avg_fill_price, total_cost, slippage_bps, fees_paid, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                decision_id,
                market_id,
                token_id,
                side,
                size,
                avg_fill_price,
                total_cost,
                slippage_bps,
                fees_paid,
                datetime.now().timestamp(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_paper_fills(
        self, cycle_id: Optional[int] = None, market_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM paper_fills WHERE 1=1"
        params: list[Any] = []

        if cycle_id is not None:
            sql += " AND cycle_id = ?"
            params.append(cycle_id)
        if market_id is not None:
            sql += " AND market_id = ?"
            params.append(market_id)

        sql += " ORDER BY timestamp DESC"
        cursor = self._conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def record_orderbook_summary(
        self,
        cycle_id: int,
        market_id: str,
        token_id: str,
        best_bid: float,
        best_ask: float,
        mid_price: float,
        spread_bps: float,
        depth_within_1pct: Optional[float] = None,
        validity_reason: Optional[str] = None,
    ) -> int:
        """Record orderbook summary."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO orderbook_summaries (cycle_id, market_id, token_id,
               best_bid, best_ask, mid_price, spread_bps, depth_within_1pct, validity_reason, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                market_id,
                token_id,
                best_bid,
                best_ask,
                mid_price,
                spread_bps,
                depth_within_1pct,
                validity_reason,
                datetime.now().timestamp(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def record_signal(
        self,
        cycle_id: int,
        market_id: str,
        token_id: str,
        side: str,
        p_implied_mid: float,
        p_implied_exec: float,
        p_fair: float,
        edge_bps: float,
        spread_bps: float,
        depth_within_1pct: Optional[float] = None,
        passed_filters: bool = True,
        reasons_json: Optional[str] = None,
    ) -> int:
        """Record a signal candidate."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO signals (cycle_id, market_id, token_id, side,
               p_implied_mid, p_implied_exec, p_fair, edge_bps, spread_bps,
               depth_within_1pct, passed_filters, reasons_json, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle_id,
                market_id,
                token_id,
                side,
                p_implied_mid,
                p_implied_exec,
                p_fair,
                edge_bps,
                spread_bps,
                depth_within_1pct,
                1 if passed_filters else 0,
                reasons_json,
                datetime.now().timestamp(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_signals_for_cycle(
        self, cycle_id: int, passed_only: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve signals for a cycle, ordered by edge descending."""
        sql = "SELECT * FROM signals WHERE cycle_id = ?"
        params: list[Any] = [cycle_id]
    
        if passed_only:
            sql += " AND passed_filters = 1"
    
        sql += " ORDER BY edge_bps DESC"
    
        if limit:
            sql += f" LIMIT {limit}"
    
        cursor = self._conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]
