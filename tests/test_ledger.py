import sqlite3
from pathlib import Path

import pytest

from src.ledger.schema import initialize_database
from src.ledger.store import Store

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
    expected = {"cycles", "account_states", "estimates", "decisions", "paper_fills"}
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
