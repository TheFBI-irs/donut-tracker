"""
historian.py — Full-market data recorder.

Records EVERY item in every API scan to SQLite — not just the watch list.
This is the raw data layer that makes macro analysis possible.

Schema:
  scans(id, ts)
  snapshots(scan_id, item_id, bbp, gap_pct, top_bid, second_bid, third_bid,
            total_bids_above_floor, raw_bid_count)

No data is thrown away. The macro module queries this to find patterns
across the entire economy — correlations, regime shifts, leading indicators.
"""

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_FILE       = "market_history.db"
MIN_BID_FLOOR = 1000
TOP_N_BIDS    = 10


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    with _connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                ts   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id               INTEGER NOT NULL REFERENCES scans(id),
                item_id               TEXT    NOT NULL,
                bbp                   REAL,
                gap_pct               REAL,
                top_bid               INTEGER,
                second_bid            INTEGER,
                third_bid             INTEGER,
                total_bids_above_floor INTEGER,
                raw_bid_count         INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_item
                ON snapshots(item_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_scan
                ON snapshots(scan_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_item_scan
                ON snapshots(item_id, scan_id);
        """)
    logger.info(f"Database ready: {DB_FILE}")


def record_scan(orders: list) -> int:
    """
    Process a full order list, compute per-item metrics for ALL items,
    and write one scan + N snapshots to the database.
    Returns the scan_id.
    """
    # Aggregate bids per item (all items, no watch-list filter)
    item_bids: dict[str, list] = {}
    for order in orders:
        item = order["item"]["itemId"]
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue
        item_bids.setdefault(item, []).append(order["itemPrice"])

    now = datetime.utcnow().isoformat()

    with _connect() as conn:
        cursor = conn.execute("INSERT INTO scans (ts) VALUES (?)", (now,))
        scan_id = cursor.lastrowid

        rows = []
        for item, bids in item_bids.items():
            raw_count   = len(bids)
            real_bids   = sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)
            top_n       = real_bids[:TOP_N_BIDS]

            bbp         = sum(top_n) / len(top_n) if top_n else None
            gap_pct     = None
            if len(top_n) >= 2:
                gaps    = [top_n[i] - top_n[i+1] for i in range(len(top_n) - 1)]
                gap_pct = (sum(gaps) / len(gaps)) / top_n[0]

            rows.append((
                scan_id,
                item,
                bbp,
                gap_pct,
                real_bids[0] if len(real_bids) > 0 else None,
                real_bids[1] if len(real_bids) > 1 else None,
                real_bids[2] if len(real_bids) > 2 else None,
                len(real_bids),
                raw_count,
            ))

        conn.executemany("""
            INSERT INTO snapshots
              (scan_id, item_id, bbp, gap_pct,
               top_bid, second_bid, third_bid,
               total_bids_above_floor, raw_bid_count)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, rows)

    logger.info(f"Scan {scan_id} recorded: {len(rows)} items at {now}")
    return scan_id


def get_item_history(item_id: str, limit: int = 48) -> list[dict]:
    """Return the last N snapshots for a single item, oldest first."""
    with _connect() as conn:
        rows = conn.execute("""
            SELECT s.ts, sn.bbp, sn.gap_pct, sn.top_bid,
                   sn.second_bid, sn.third_bid, sn.total_bids_above_floor
            FROM snapshots sn
            JOIN scans s ON s.id = sn.scan_id
            WHERE sn.item_id = ?
            ORDER BY sn.scan_id DESC
            LIMIT ?
        """, (item_id, limit)).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_all_items_latest() -> list[dict]:
    """Return the most recent snapshot for every item."""
    with _connect() as conn:
        rows = conn.execute("""
            SELECT sn.item_id, s.ts, sn.bbp, sn.gap_pct,
                   sn.top_bid, sn.total_bids_above_floor
            FROM snapshots sn
            JOIN scans s ON s.id = sn.scan_id
            WHERE sn.scan_id = (SELECT MAX(id) FROM scans)
        """).fetchall()
    return [dict(r) for r in rows]


def get_scan_count() -> int:
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]