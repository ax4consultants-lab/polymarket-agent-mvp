import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

CREATE_CYCLES_TABLE = """
CREATE TABLE IF NOT EXISTS cycles (
    cycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('success', 'error', 'halted')),
    markets_scanned INTEGER,
    opportunities_found INTEGER,
    decisions_made INTEGER,
    error_message TEXT,
    execution_time_ms REAL
);
"""

CREATE_ACCOUNT_STATES_TABLE = """
CREATE TABLE IF NOT EXISTS account_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL,
    timestamp REAL NOT NULL,
    equity REAL NOT NULL,
    cash REAL NOT NULL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    total_exposure REAL,
    FOREIGN KEY (cycle_id) REFERENCES cycles(cycle_id)
);
"""

CREATE_ESTIMATES_TABLE = """
CREATE TABLE IF NOT EXISTS estimates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
    fair_value REAL NOT NULL,
    market_price REAL NOT NULL,
    gross_edge_bps REAL NOT NULL,
    fee_est_bps REAL NOT NULL,
    slippage_est_bps REAL NOT NULL,
    net_edge_bps REAL NOT NULL,
    confidence REAL,
    FOREIGN KEY (cycle_id) REFERENCES cycles(cycle_id)
);
"""

CREATE_DECISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL,
    estimate_id INTEGER NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('trade', 'skip')),
    reason TEXT,
    kelly_fraction REAL,
    target_size REAL,
    target_price REAL,
    FOREIGN KEY (cycle_id) REFERENCES cycles(cycle_id),
    FOREIGN KEY (estimate_id) REFERENCES estimates(id)
);
"""

CREATE_PAPER_FILLS_TABLE = """
CREATE TABLE IF NOT EXISTS paper_fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL,
    decision_id INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    size REAL NOT NULL,
    avg_fill_price REAL NOT NULL,
    total_cost REAL NOT NULL,
    slippage_bps REAL,
    fees_paid REAL,
    timestamp REAL NOT NULL,
    FOREIGN KEY (cycle_id) REFERENCES cycles(cycle_id),
    FOREIGN KEY (decision_id) REFERENCES decisions(id)
);
"""

CREATE_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_cycles_timestamp ON cycles(timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_account_states_cycle ON account_states(cycle_id);",
    "CREATE INDEX IF NOT EXISTS idx_estimates_cycle ON estimates(cycle_id);",
    "CREATE INDEX IF NOT EXISTS idx_estimates_market ON estimates(market_id, token_id);",
    "CREATE INDEX IF NOT EXISTS idx_decisions_cycle ON decisions(cycle_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_fills_cycle ON paper_fills(cycle_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_fills_market ON paper_fills(market_id, token_id);",
]

def initialize_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(CREATE_CYCLES_TABLE)
    cursor.execute(CREATE_ACCOUNT_STATES_TABLE)
    cursor.execute(CREATE_ESTIMATES_TABLE)
    cursor.execute(CREATE_DECISIONS_TABLE)
    cursor.execute(CREATE_PAPER_FILLS_TABLE)

    for sql in CREATE_INDICES:
        cursor.execute(sql)

    conn.commit()
    conn.close()
