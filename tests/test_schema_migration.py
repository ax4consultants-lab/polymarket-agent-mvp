import sqlite3
from pathlib import Path

from src.ledger.schema import initialize_database
from src.ledger.store import Store


def test_initialize_database_adds_validity_reason_and_persists_it(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"

    # Simulate legacy DB: orderbook_summaries exists but without validity_reason.
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE orderbook_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id INTEGER NOT NULL,
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            best_bid REAL NOT NULL,
            best_ask REAL NOT NULL,
            mid_price REAL NOT NULL,
            spread_bps REAL NOT NULL,
            depth_within_1pct REAL,
            timestamp REAL NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    # Should run create-if-missing + guarded ALTER for pre-existing table.
    initialize_database(db_path)

    conn = sqlite3.connect(db_path)
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(orderbook_summaries)").fetchall()
    }
    assert "validity_reason" in columns
    conn.close()

    with Store(db_path) as store:
        cycle_id = store.create_cycle(1234567890.0, "success")
        row_id = store.record_orderbook_summary(
            cycle_id=cycle_id,
            market_id="mkt-1",
            token_id="tok-1",
            best_bid=0.42,
            best_ask=0.44,
            mid_price=0.43,
            spread_bps=476.19,
            depth_within_1pct=100.0,
            validity_reason="missing_asks",
        )
        assert row_id > 0

    conn = sqlite3.connect(db_path)
    saved_reason = conn.execute(
        "SELECT validity_reason FROM orderbook_summaries WHERE id = ?",
        (row_id,),
    ).fetchone()[0]
    conn.close()

    assert saved_reason == "missing_asks"
