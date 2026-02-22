"""
historian.py — Full-market data recorder using PostgreSQL.

Connects via DATABASE_URL environment variable injected by Railway.
Schema is identical to the SQLite version — only connection and
placeholder syntax changed.
"""

import os
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

MIN_BID_FLOOR = 1000
TOP_N_BIDS    = 10
DEPTH_FLOOR   = 0.90


def _connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set.")
    conn = psycopg2.connect(url)
    return conn


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                id  SERIAL PRIMARY KEY,
                ts  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    id  SERIAL PRIMARY KEY,
                    ts  TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id                      SERIAL PRIMARY KEY,
                    scan_id                 INTEGER NOT NULL REFERENCES scans(id),
                    item_id                 TEXT    NOT NULL,
                    bbp                     REAL,
                    gap_pct                 REAL,
                    depth                   INTEGER,
                    top_bid                 BIGINT,
                    second_bid              BIGINT,
                    third_bid               BIGINT,
                    total_bids_above_floor  INTEGER,
                    raw_bid_count           INTEGER
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_item
                    ON snapshots(item_id);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_scan
                    ON snapshots(scan_id);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_item_scan
                    ON snapshots(item_id, scan_id);
            """)
        conn.commit()
    logger.info("PostgreSQL database ready.")


def compute_depth(real_bids: list, bbp: float) -> int:
    if not bbp:
        return 0
    floor = bbp * DEPTH_FLOOR
    return sum(1 for b in real_bids if b >= floor)


def record_scan(orders: list) -> int:
    """Process full order list and write one scan + N snapshots."""
    item_bids: dict[str, list] = {}
    for order in orders:
        item      = order["item"]["itemId"]
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue
        item_bids.setdefault(item, []).append(order["itemPrice"])

    now = datetime.utcnow().isoformat()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO scans (ts) VALUES (%s) RETURNING id", (now,))
            scan_id = cur.fetchone()[0]

            rows = []
            for item, bids in item_bids.items():
                raw_count = len(bids)
                real_bids = sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)
                top_n     = real_bids[:TOP_N_BIDS]

                bbp     = sum(top_n) / len(top_n) if top_n else None
                gap_pct = None
                if len(top_n) >= 2:
                    gaps    = [top_n[i] - top_n[i+1] for i in range(len(top_n) - 1)]
                    gap_pct = (sum(gaps) / len(gaps)) / top_n[0]

                depth = compute_depth(real_bids, bbp) if bbp else 0

                rows.append((
                    scan_id, item, bbp, gap_pct, depth,
                    real_bids[0] if len(real_bids) > 0 else None,
                    real_bids[1] if len(real_bids) > 1 else None,
                    real_bids[2] if len(real_bids) > 2 else None,
                    len(real_bids), raw_count,
                ))

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO snapshots
                  (scan_id, item_id, bbp, gap_pct, depth,
                   top_bid, second_bid, third_bid,
                   total_bids_above_floor, raw_bid_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)

        conn.commit()

    logger.info(f"Scan {scan_id} recorded: {len(rows)} items at {now}")
    return scan_id


def get_item_history(item_id: str, limit: int = 48) -> list[dict]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.ts, sn.bbp, sn.gap_pct, sn.depth,
                       sn.top_bid, sn.second_bid, sn.third_bid,
                       sn.total_bids_above_floor
                FROM snapshots sn
                JOIN scans s ON s.id = sn.scan_id
                WHERE sn.item_id = %s
                ORDER BY sn.scan_id DESC
                LIMIT %s
            """, (item_id, limit))
            rows = cur.fetchall()
    return list(reversed([dict(r) for r in rows]))


def get_all_items_latest() -> list[dict]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT sn.item_id, s.ts, sn.bbp, sn.gap_pct,
                       sn.depth, sn.top_bid, sn.total_bids_above_floor
                FROM snapshots sn
                JOIN scans s ON s.id = sn.scan_id
                WHERE sn.scan_id = (SELECT MAX(id) FROM scans)
            """)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_scan_count() -> int:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM scans")
            return cur.fetchone()[0]


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """
    General-purpose query helper for macro.py and digest.py.
    Returns list of dicts. Use %s placeholders.
    """
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows]