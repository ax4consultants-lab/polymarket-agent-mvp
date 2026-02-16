import sqlite3
from pathlib import Path

import pytest

from src.core.types import OrderBook
from src.ledger.schema import initialize_database
from src.ledger.store import Store
from src.strategy.signal_generation import create_signal_candidate


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    initialize_database(db_path)
    return db_path


def test_schema_creation(temp_db: Path) -> None:
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    expected = {
        "cycles",
        "account_states",
        "estimates",
        "decisions",
        "paper_fills",
        "signals",
    }
    assert expected.issubset(tables)
    conn.close()


def test_store_cycle_and_account_state(temp_db: Path) -> None:
    with Store(temp_db) as store:
        cycle_id = store.create_cycle(1234567890.0, "success", markets_scanned=10)
        assert cycle_id > 0
        store.update_cycle(cycle_id, status="halted", error_message="test")
        state_id = store.record_account_state(
            cycle_id, equity=10000.0, cash=9500.0, total_exposure=500.0
        )
        assert state_id > 0
        latest = store.get_latest_account_state()
        assert latest is not None
        assert latest["equity"] == 10000.0
        assert latest["cash"] == 9500.0


def test_signal_candidate_uses_side_specific_executable_prices() -> None:
    orderbook = OrderBook(
        market_id="mkt-1",
        token_id="tok-1",
        bids=[],
        asks=[],
        best_bid=0.45,
        best_ask=0.55,
        mid_price=0.50,
        spread_bps=2000.0,
        timestamp=1234567890.0,
    )

    buy_candidate = create_signal_candidate(orderbook=orderbook, fair_value=0.60, side="buy")
    sell_candidate = create_signal_candidate(orderbook=orderbook, fair_value=0.40, side="sell")

    assert buy_candidate.implied_prices.p_implied_mid == 0.50
    assert buy_candidate.implied_prices.p_implied_exec_buy == 0.55
    assert buy_candidate.implied_prices.p_implied_exec_sell == 0.45
    assert buy_candidate.ranking_price == 0.55
    assert sell_candidate.ranking_price == 0.45


def test_record_signal_persists_implied_prices(temp_db: Path) -> None:
    with Store(temp_db) as store:
        cycle_id = store.create_cycle(1234567890.0, "success", markets_scanned=1)
        signal_id = store.record_signal(
            cycle_id=cycle_id,
            market_id="mkt-1",
            token_id="tok-1",
            side="buy",
            fair_value=0.6,
            edge_bps=200.0,
            ranking_price=0.55,
            p_implied_mid=0.50,
            p_implied_exec_buy=0.55,
            p_implied_exec_sell=0.45,
        )

        assert signal_id > 0

        row = store._conn.execute(
            "SELECT p_implied_mid, p_implied_exec_buy, p_implied_exec_sell FROM signals WHERE id = ?",
            (signal_id,),
        ).fetchone()

        assert row is not None
        assert row["p_implied_mid"] == 0.50
        assert row["p_implied_exec_buy"] == 0.55
        assert row["p_implied_exec_sell"] == 0.45
