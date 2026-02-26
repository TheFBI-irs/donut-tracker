"""
historian.py — Full-market data recorder using PostgreSQL.

Tables:
  scans       — one row per API fetch cycle
  snapshots   — one row per item per scan (aggregated market metrics)
  orders_raw  — one row per individual order per scan (all API fields)
                fingerprint = hash(userName + creationDate + itemId + price)
                enables cross-scan order tracking for cancel/fill/requote
  events      — manually logged structural events

NEW in this version:
  orders_raw stores every individual order with userName, creationDate,
  expirationDate, lastUpdated, enchantments. This unlocks:
    - Whale concentration (HHI by userName)
    - Order age (stale vs fresh bids)
    - Time-to-expiry pressure
    - Cancel vs fill classification
    - Requote intensity
    - Implied trade volume (delta of amountDelivered)
"""

import os
import hashlib
import json
import logging
from datetime import datetime, timezone

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
    return psycopg2.connect(url)


def _order_fingerprint(order: dict) -> str:
    """
    Stable identifier for an order across scans.
    Uses userName + creationDate + itemId + price since the API
    has no explicit order ID.
    """
    key = (
        order.get("userName", "") +
        order.get("creationDate", "") +
        order["item"]["itemId"] +
        str(order["itemPrice"])
    )
    return hashlib.md5(key.encode()).hexdigest()


def init_db():
    """Create all tables if they don't exist. Safe to call on every startup."""
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
                    raw_bid_count           INTEGER,
                    -- New aggregated metrics from orders_raw
                    whale_hhi               REAL,       -- Herfindahl-Hirschman Index of top bidders
                    top_bidder              TEXT,       -- userName of highest bidder
                    avg_order_age_hours     REAL,       -- avg age of top N orders in hours
                    tte_pressure            REAL,       -- fraction of top depth expiring within 24h
                    implied_volume          BIGINT,     -- sum of new amountDelivered since last scan
                    cancel_count            INTEGER,    -- orders that disappeared with low fill
                    fill_count              INTEGER,    -- orders that disappeared with high fill
                    requote_count           INTEGER,    -- orders updated (lastUpdated changed)
                    depth_volatility        REAL        -- std dev of depth across last 6 scans
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_raw (
                    id              SERIAL PRIMARY KEY,
                    scan_id         INTEGER NOT NULL REFERENCES scans(id),
                    fingerprint     TEXT    NOT NULL,
                    item_id         TEXT    NOT NULL,
                    user_name       TEXT,
                    item_price      BIGINT  NOT NULL,
                    amount_ordered  INTEGER,
                    amount_delivered INTEGER,
                    creation_date   TIMESTAMPTZ,
                    expiration_date TIMESTAMPTZ,
                    last_updated    TIMESTAMPTZ,
                    enchantments    JSONB,
                    is_above_floor  BOOLEAN
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
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_snapshots_item ON snapshots(item_id);",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_scan ON snapshots(scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_item_scan ON snapshots(item_id, scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_orders_scan ON orders_raw(scan_id);",
                "CREATE INDEX IF NOT EXISTS idx_orders_item ON orders_raw(item_id);",
                "CREATE INDEX IF NOT EXISTS idx_orders_fingerprint ON orders_raw(fingerprint);",
                "CREATE INDEX IF NOT EXISTS idx_orders_user ON orders_raw(user_name);",
                "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);",
            ]:
                cur.execute(idx_sql)
        conn.commit()
    logger.info("PostgreSQL database ready.")


def compute_depth(real_bids: list, bbp: float) -> int:
    if not bbp:
        return 0
    return sum(1 for b in real_bids if b >= bbp * DEPTH_FLOOR)


def _parse_ts(ts_str: str | None):
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _compute_whale_hhi(orders_above_floor: list) -> float | None:
    """
    Herfindahl-Hirschman Index of bid notional concentration by userName.
    HHI = sum of (share_i)^2 where share_i = user_i's notional / total notional.
    Range 0-1. High = few players dominate. Low = spread across many players.
    """
    if not orders_above_floor:
        return None
    notional_by_user = {}
    total_notional   = 0
    for o in orders_above_floor:
        user     = o.get("userName", "unknown")
        notional = o["itemPrice"] * (o["amountOrdered"] - o["amountDelivered"])
        notional_by_user[user] = notional_by_user.get(user, 0) + notional
        total_notional += notional
    if total_notional == 0:
        return None
    return sum((n / total_notional) ** 2 for n in notional_by_user.values())


def _compute_tte_pressure(top_orders: list, now: datetime) -> float | None:
    """
    Fraction of top-N depth with expiration within 24 hours.
    High value = many bids about to expire = potential depth collapse.
    """
    if not top_orders:
        return None
    expiring = 0
    for o in top_orders:
        exp = _parse_ts(o.get("expirationDate"))
        if exp and (exp - now).total_seconds() < 86400:
            expiring += 1
    return expiring / len(top_orders)


def _compute_avg_age(top_orders: list, now: datetime) -> float | None:
    """Average age of top-N orders in hours."""
    ages = []
    for o in top_orders:
        created = _parse_ts(o.get("creationDate"))
        if created:
            ages.append((now - created).total_seconds() / 3600)
    return sum(ages) / len(ages) if ages else None


def _compute_implied_volume(item_id: str, scan_id: int,
                             current_orders: list) -> int:
    """
    Sum of increases in amountDelivered for orders seen in previous scan too.
    This is actual executed trade volume since last scan.
    """
    curr_fingerprints = {_order_fingerprint(o): o["amountDelivered"]
                         for o in current_orders}

    prev = execute_query("""
        SELECT fingerprint, amount_delivered
        FROM orders_raw
        WHERE item_id = %s
          AND scan_id = (
              SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1
          )
    """, (item_id, scan_id))

    if not prev:
        return 0

    volume = 0
    for row in prev:
        fp   = row["fingerprint"]
        prev_delivered = row["amount_delivered"] or 0
        curr_delivered = curr_fingerprints.get(fp, prev_delivered)
        delta = curr_delivered - prev_delivered
        if delta > 0:
            volume += delta
    return volume


def _compute_cancel_fill(item_id: str, scan_id: int,
                          current_fps: set) -> tuple[int, int]:
    """
    Compare previous scan's orders to current.
    Disappeared orders: high fill ratio → fill, low fill ratio → cancel.
    Returns (cancel_count, fill_count).
    """
    prev = execute_query("""
        SELECT fingerprint, amount_ordered, amount_delivered
        FROM orders_raw
        WHERE item_id = %s
          AND scan_id = (
              SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1
          )
    """, (item_id, scan_id))

    cancels = 0
    fills   = 0
    for row in prev:
        if row["fingerprint"] not in current_fps:
            amt_ord  = row["amount_ordered"]  or 1
            amt_del  = row["amount_delivered"] or 0
            fill_ratio = amt_del / amt_ord
            if fill_ratio >= 0.8:
                fills   += 1
            else:
                cancels += 1
    return cancels, fills


def _compute_requote_count(item_id: str, scan_id: int,
                            current_orders: list) -> int:
    """
    Orders whose lastUpdated changed since previous scan = active repricing.
    High requote count = information arrival, players adjusting views.
    """
    curr_map = {_order_fingerprint(o): o.get("lastUpdated", "")
                for o in current_orders}

    prev = execute_query("""
        SELECT fingerprint, last_updated
        FROM orders_raw
        WHERE item_id = %s
          AND scan_id = (
              SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1
          )
    """, (item_id, scan_id))

    requotes = 0
    for row in prev:
        fp        = row["fingerprint"]
        prev_upd  = str(row.get("last_updated", ""))
        curr_upd  = curr_map.get(fp, "")
        if fp in curr_map and curr_upd != prev_upd and curr_upd:
            requotes += 1
    return requotes


def record_scan(orders: list) -> int:
    """
    Process full order list:
      1. Write scans row
      2. Write orders_raw rows (all individual orders with full fields)
      3. Write snapshots rows (aggregated per item, including new metrics)
    Returns scan_id.
    """
    now = datetime.now(timezone.utc)

    # Group by item
    item_orders: dict[str, list] = {}
    for order in orders:
        item      = order["item"]["itemId"]
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue
        item_orders.setdefault(item, []).append(order)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO scans DEFAULT VALUES RETURNING id")
            scan_id = cur.fetchone()[0]

            # --- orders_raw ---
            raw_rows = []
            for item, item_order_list in item_orders.items():
                for o in item_order_list:
                    enc = o["item"].get("enchantments")
                    raw_rows.append((
                        scan_id,
                        _order_fingerprint(o),
                        item,
                        o.get("userName"),
                        o["itemPrice"],
                        o.get("amountOrdered"),
                        o.get("amountDelivered"),
                        _parse_ts(o.get("creationDate")),
                        _parse_ts(o.get("expirationDate")),
                        _parse_ts(o.get("lastUpdated")),
                        json.dumps(enc) if enc else None,
                        o["itemPrice"] >= MIN_BID_FLOOR,
                    ))

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO orders_raw
                  (scan_id, fingerprint, item_id, user_name,
                   item_price, amount_ordered, amount_delivered,
                   creation_date, expiration_date, last_updated,
                   enchantments, is_above_floor)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, raw_rows)

            # --- snapshots with enhanced metrics ---
            snap_rows = []
            for item, item_order_list in item_orders.items():
                all_bids  = [o["itemPrice"] for o in item_order_list]
                real_bids = sorted([b for b in all_bids if b >= MIN_BID_FLOOR],
                                   reverse=True)
                top_n     = real_bids[:TOP_N_BIDS]

                bbp     = sum(top_n) / len(top_n) if top_n else None
                gap_pct = None
                if len(top_n) >= 2:
                    gaps    = [top_n[i] - top_n[i+1] for i in range(len(top_n) - 1)]
                    gap_pct = (sum(gaps) / len(gaps)) / top_n[0]

                depth = compute_depth(real_bids, bbp) if bbp else 0

                # Orders above floor for whale/age/TTE metrics
                above_floor = [o for o in item_order_list
                                if o["itemPrice"] >= MIN_BID_FLOOR]
                top_orders  = sorted(above_floor,
                                     key=lambda o: o["itemPrice"],
                                     reverse=True)[:TOP_N_BIDS]

                whale_hhi   = _compute_whale_hhi(above_floor)
                top_bidder  = top_orders[0].get("userName") if top_orders else None
                avg_age     = _compute_avg_age(top_orders, now)
                tte_pressure= _compute_tte_pressure(top_orders, now)

                # Cross-scan metrics (require previous scan data)
                current_fps = {_order_fingerprint(o) for o in item_order_list}
                implied_vol = _compute_implied_volume(item, scan_id, item_order_list)
                cancels, fills = _compute_cancel_fill(item, scan_id, current_fps)
                requotes    = _compute_requote_count(item, scan_id, item_order_list)

                snap_rows.append((
                    scan_id, item, bbp, gap_pct, depth,
                    real_bids[0] if len(real_bids) > 0 else None,
                    real_bids[1] if len(real_bids) > 1 else None,
                    real_bids[2] if len(real_bids) > 2 else None,
                    len(real_bids), len(all_bids),
                    whale_hhi, top_bidder, avg_age, tte_pressure,
                    implied_vol, cancels, fills, requotes,
                    None,  # depth_volatility filled by features.py
                ))

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO snapshots
                  (scan_id, item_id, bbp, gap_pct, depth,
                   top_bid, second_bid, third_bid,
                   total_bids_above_floor, raw_bid_count,
                   whale_hhi, top_bidder, avg_order_age_hours,
                   tte_pressure, implied_volume,
                   cancel_count, fill_count, requote_count,
                   depth_volatility)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                   %s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, snap_rows)

        conn.commit()

    logger.info(f"Scan {scan_id} recorded: {len(snap_rows)} items, "
                f"{len(raw_rows)} individual orders")
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
                SELECT s.ts, sn.bbp, sn.gap_pct, sn.depth,
                       sn.top_bid, sn.second_bid, sn.third_bid,
                       sn.total_bids_above_floor, sn.whale_hhi,
                       sn.top_bidder, sn.avg_order_age_hours,
                       sn.tte_pressure, sn.implied_volume,
                       sn.cancel_count, sn.fill_count, sn.requote_count
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
                       sn.depth, sn.top_bid, sn.total_bids_above_floor,
                       sn.whale_hhi, sn.top_bidder
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