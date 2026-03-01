"""
historian.py — Full-market data recorder using PostgreSQL.

UPDATED FOR NEW API (api.donutsmp.net):
  Old API: buy orders (bids) → BBP = best bid price
  New API: sell listings (asks) → BAP = best ask price
           + completed transactions → actual trade prices

Tables:
  scans        — one row per API fetch cycle
  snapshots    — one row per item per scan (aggregated sell-side metrics)
  transactions — completed sales (price, timestamp, item, seller)
  orders_raw   — individual listings per scan
  events       — manually logged structural events

Key metrics per item per scan:
  bap          — best ask price (lowest listing price)
  ask_depth    — number of listings within 10% of BAP
  vwap_24h     — volume-weighted avg price from transactions last 24h
  sale_count   — number of sales in last 24h from transactions
  listing_count — total active listings for this item
"""

import os
import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

MIN_LISTING_FLOOR = 1000
TOP_N_LISTINGS    = 10
DEPTH_FLOOR       = 1.10   # within 10% ABOVE BAP (ask side)


def _connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set.")
    return psycopg2.connect(url)


def _listing_fingerprint(listing: dict) -> str:
    key = (
        listing.get("seller", {}).get("uuid", "") +
        listing["item"]["id"] +
        str(listing["price"]) +
        str(listing.get("time_left", ""))
    )
    return hashlib.md5(key.encode()).hexdigest()


def _item_id(listing: dict) -> str:
    """Strip minecraft: prefix for consistency."""
    return listing["item"]["id"].replace("minecraft:", "")


def init_db():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    id   SERIAL PRIMARY KEY,
                    ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id               SERIAL PRIMARY KEY,
                    scan_id          INTEGER NOT NULL REFERENCES scans(id),
                    item_id          TEXT    NOT NULL,
                    bap              REAL,        -- best ask price (lowest listing)
                    ask_depth        INTEGER,     -- listings within 10% of BAP
                    listing_count    INTEGER,     -- total active listings
                    top_ask          BIGINT,      -- lowest listing price
                    second_ask       BIGINT,
                    third_ask        BIGINT,
                    ask_gap_pct      REAL,        -- gap between top asks (demand proxy)
                    top_seller       TEXT,        -- who has the lowest listing
                    whale_hhi        REAL,        -- listing concentration by seller
                    min_time_left    BIGINT,      -- time_left of lowest listing (ms)
                    avg_time_left    REAL,        -- avg time_left of top N listings
                    vwap_24h         REAL,        -- volume weighted avg price last 24h
                    sale_count_24h   INTEGER,     -- number of sales last 24h
                    volume_24h       BIGINT       -- total coins volume last 24h
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id               SERIAL PRIMARY KEY,
                    scan_id          INTEGER NOT NULL REFERENCES scans(id),
                    item_id          TEXT    NOT NULL,
                    seller_name      TEXT,
                    seller_uuid      TEXT,
                    price            BIGINT  NOT NULL,
                    sold_at          TIMESTAMPTZ,
                    item_count       INTEGER,
                    enchantments     JSONB
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_raw (
                    id               SERIAL PRIMARY KEY,
                    scan_id          INTEGER NOT NULL REFERENCES scans(id),
                    fingerprint      TEXT    NOT NULL,
                    item_id          TEXT    NOT NULL,
                    seller_name      TEXT,
                    seller_uuid      TEXT,
                    price            BIGINT  NOT NULL,
                    time_left        BIGINT,
                    item_count       INTEGER,
                    enchantments     JSONB
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id          SERIAL PRIMARY KEY,
                    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    event_type  TEXT NOT NULL,
                    description TEXT,
                    severity    TEXT DEFAULT 'major',
                    logged_by   TEXT DEFAULT 'manual'
                );
            """)
            for idx in [
                "CREATE INDEX IF NOT EXISTS idx_snapshots_item ON snapshots(item_id);",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_scan ON snapshots(scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_item_scan ON snapshots(item_id, scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_transactions_item ON transactions(item_id);",
                "CREATE INDEX IF NOT EXISTS idx_transactions_sold ON transactions(sold_at);",
                "CREATE INDEX IF NOT EXISTS idx_orders_scan ON orders_raw(scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_orders_item ON orders_raw(item_id);",
                "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);",
            ]:
                cur.execute(idx)
        conn.commit()
    logger.info("PostgreSQL database ready.")


def _compute_vwap_and_volume(item_id: str, transactions: list, since_ts: datetime):
    """Compute VWAP and sale count from transactions for an item since given time."""
    relevant = [
        t for t in transactions
        if t["item"]["id"].replace("minecraft:", "") == item_id
        and t.get("unixMillisDateSold", 0) >= since_ts.timestamp() * 1000
    ]
    if not relevant:
        return None, 0, 0

    total_volume = sum(t["price"] * t["item"].get("count", 1) for t in relevant)
    total_units  = sum(t["item"].get("count", 1) for t in relevant)
    vwap         = total_volume / total_units if total_units > 0 else None
    sale_count   = len(relevant)
    return vwap, sale_count, total_volume


def record_scan(listings: list, transactions: list) -> int:
    now      = datetime.now(timezone.utc)
    since_24 = now - timedelta(hours=24)

    # Group listings by item
    item_listings: dict[str, list] = {}
    for listing in listings:
        item = _item_id(listing)
        if listing["price"] < MIN_LISTING_FLOOR:
            continue
        item_listings.setdefault(item, []).append(listing)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO scans DEFAULT VALUES RETURNING id")
            scan_id = cur.fetchone()[0]

            # --- transactions table ---
            tx_rows = []
            for t in transactions:
                item = _item_id(t)
                sold_ts = None
                if t.get("unixMillisDateSold"):
                    sold_ts = datetime.fromtimestamp(
                        t["unixMillisDateSold"] / 1000, tz=timezone.utc
                    )
                enc = t["item"].get("enchants")
                tx_rows.append((
                    scan_id, item,
                    t["seller"]["name"], t["seller"]["uuid"],
                    t["price"], sold_ts,
                    t["item"].get("count", 1),
                    json.dumps(enc) if enc else None,
                ))

            if tx_rows:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO transactions
                      (scan_id, item_id, seller_name, seller_uuid,
                       price, sold_at, item_count, enchantments)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, tx_rows)

            # --- orders_raw table ---
            raw_rows = []
            for item, item_list in item_listings.items():
                for listing in item_list:
                    enc = listing["item"].get("enchants")
                    raw_rows.append((
                        scan_id,
                        _listing_fingerprint(listing),
                        item,
                        listing["seller"]["name"],
                        listing["seller"]["uuid"],
                        listing["price"],
                        listing.get("time_left"),
                        listing["item"].get("count", 1),
                        json.dumps(enc) if enc else None,
                    ))

            if raw_rows:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO orders_raw
                      (scan_id, fingerprint, item_id, seller_name, seller_uuid,
                       price, time_left, item_count, enchantments)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, raw_rows)

            # --- snapshots table ---
            snap_rows = []
            for item, item_list in item_listings.items():
                sorted_listings = sorted(item_list, key=lambda x: x["price"])
                top_n           = sorted_listings[:TOP_N_LISTINGS]
                prices          = [l["price"] for l in top_n]

                bap       = prices[0] if prices else None
                ask_depth = sum(1 for p in prices if p <= bap * DEPTH_FLOOR) if bap else 0

                ask_gap_pct = None
                if len(prices) >= 2:
                    gaps = [prices[i+1] - prices[i] for i in range(len(prices)-1)]
                    ask_gap_pct = (sum(gaps) / len(gaps)) / prices[0]

                # Whale HHI by seller notional
                notional_by_seller = {}
                total_notional     = 0
                for l in sorted_listings:
                    seller   = l["seller"]["name"]
                    notional = l["price"] * l["item"].get("count", 1)
                    notional_by_seller[seller] = notional_by_seller.get(seller, 0) + notional
                    total_notional += notional
                whale_hhi = (
                    sum((n/total_notional)**2 for n in notional_by_seller.values())
                    if total_notional > 0 else None
                )

                time_lefts   = [l.get("time_left", 0) for l in top_n if l.get("time_left")]
                min_tl       = min(time_lefts) if time_lefts else None
                avg_tl       = sum(time_lefts) / len(time_lefts) if time_lefts else None

                vwap, sale_count, volume = _compute_vwap_and_volume(
                    item, transactions, since_24
                )

                snap_rows.append((
                    scan_id, item, bap, ask_depth, len(sorted_listings),
                    prices[0] if len(prices) > 0 else None,
                    prices[1] if len(prices) > 1 else None,
                    prices[2] if len(prices) > 2 else None,
                    ask_gap_pct,
                    top_n[0]["seller"]["name"] if top_n else None,
                    whale_hhi, min_tl, avg_tl,
                    vwap, sale_count, volume,
                ))

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO snapshots
                  (scan_id, item_id, bap, ask_depth, listing_count,
                   top_ask, second_ask, third_ask, ask_gap_pct,
                   top_seller, whale_hhi, min_time_left, avg_time_left,
                   vwap_24h, sale_count_24h, volume_24h)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, snap_rows)

        conn.commit()

    logger.info(
        f"Scan {scan_id} recorded: {len(snap_rows)} items, "
        f"{len(raw_rows)} listings, {len(tx_rows)} transactions"
    )
    return scan_id


def log_event(event_type: str, description: str, severity: str = "major"):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO events (event_type, description, severity)
                VALUES (%s, %s, %s) RETURNING id
            """, (event_type, description, severity))
            event_id = cur.fetchone()[0]
        conn.commit()
    logger.info(f"Event logged: [{severity}] {event_type} — {description}")
    return event_id


def get_recent_events(limit: int = 5) -> list[dict]:
    return execute_query("SELECT * FROM events ORDER BY ts DESC LIMIT %s", (limit,))


def get_scans_since_event(event_ts) -> int:
    rows = execute_query(
        "SELECT COUNT(*) AS n FROM scans WHERE ts > %s", (event_ts,)
    )
    return rows[0]["n"] if rows else 0


def get_item_history(item_id: str, limit: int = 48) -> list[dict]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.ts, sn.bap, sn.ask_depth, sn.listing_count,
                       sn.top_ask, sn.second_ask, sn.third_ask,
                       sn.ask_gap_pct, sn.top_seller, sn.whale_hhi,
                       sn.vwap_24h, sn.sale_count_24h, sn.volume_24h
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
                SELECT sn.item_id, s.ts, sn.bap, sn.ask_depth,
                       sn.listing_count, sn.top_ask, sn.vwap_24h,
                       sn.sale_count_24h, sn.whale_hhi
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
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows]