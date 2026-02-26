"""
features.py — ML feature extraction pipeline.

Full 34-feature set across 9 categories:

  PRICE MOMENTUM (5)
  bbp_change_1, bbp_change_3, bbp_change_6
  bbp_volatility_6        std dev / mean of BBP over last 6 scans
  trend_duration          consecutive scans in same direction

  DEMAND / ORDER BOOK (7)
  gap_pct, gap_change_1, gap_z
  depth, depth_change_1, depth_z
  depth_acceleration      2nd derivative of depth

  WHALE DETECTION (4)
  top_bid_change_1        % change in single highest bid
  top_bid_vs_bbp          top_bid / BBP
  whale_hhi               Herfindahl-Hirschman Index of bid concentration by user
  whale_hhi_change_1      delta in HHI vs previous scan

  ORDER LIFECYCLE (5)
  avg_order_age_hours     avg age of top-N bids in hours
  tte_pressure            fraction of top depth expiring within 24h
  implied_volume          executed trade volume since last scan
  cancel_count            orders that disappeared with low fill ratio
  fill_count              orders that disappeared with high fill ratio

  ACTIVITY (1)
  requote_count           orders repriced since last scan

  DIVERGENCE (1)
  depth_bbp_divergence    sign(depth_change) * sign(bbp_change): -1 = warning

  CROSS-SECTIONAL (3)
  breadth                 % of all items rising this scan
  item_vs_breadth         bbp_change_1 - breadth (alpha vs market)
  breadth_change_1        breadth delta vs previous scan

  EVENT FEATURES (2)
  scans_since_event       scans since last major logged event
  is_post_event           1 if within 10 scans of a major event

  TIME (2)
  hour_of_day, is_weekend

  LABEL
  label                   next scan's bbp_change_1 (backfilled retroactively)
"""

import logging
from datetime import datetime, timezone

from historian import execute_query, _connect

logger = logging.getLogger(__name__)

FEATURE_MIN_HISTORY = 7
CALIBRATION_WINDOW  = 48
POST_EVENT_WINDOW   = 10


def init_features_table():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS features (
                    id                   SERIAL PRIMARY KEY,
                    scan_id              INTEGER NOT NULL REFERENCES scans(id),
                    item_id              TEXT    NOT NULL,

                    bbp_change_1         REAL,
                    bbp_change_3         REAL,
                    bbp_change_6         REAL,
                    bbp_volatility_6     REAL,
                    trend_duration       INTEGER,

                    gap_pct              REAL,
                    gap_change_1         REAL,
                    gap_z                REAL,
                    depth                INTEGER,
                    depth_change_1       REAL,
                    depth_z              REAL,
                    depth_acceleration   REAL,

                    top_bid_change_1     REAL,
                    top_bid_vs_bbp       REAL,
                    whale_hhi            REAL,
                    whale_hhi_change_1   REAL,

                    avg_order_age_hours  REAL,
                    tte_pressure         REAL,
                    implied_volume       BIGINT,
                    cancel_count         INTEGER,
                    fill_count           INTEGER,

                    requote_count        INTEGER,

                    depth_bbp_divergence REAL,

                    breadth              REAL,
                    item_vs_breadth      REAL,
                    breadth_change_1     REAL,

                    scans_since_event    INTEGER,
                    is_post_event        INTEGER,

                    hour_of_day          INTEGER,
                    is_weekend           INTEGER,

                    label                REAL DEFAULT NULL
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_features_item ON features(item_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_features_scan ON features(scan_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_features_label ON features(label) WHERE label IS NOT NULL;")
        conn.commit()
    logger.info("Features table ready.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(new, old):
    if new is None or old is None or old == 0:
        return None
    return (new - old) / old

def _sign(x):
    if x is None: return 0.0
    return 1.0 if x > 0 else (-1.0 if x < 0 else 0.0)

def _zscore(value, window):
    if len(window) < 5: return None
    avg = sum(window) / len(window)
    std = (sum((x - avg)**2 for x in window) / len(window)) ** 0.5
    return (value - avg) / std if std > 0 else 0.0

def _volatility(values):
    if len(values) < 3: return None
    avg = sum(values) / len(values)
    if avg == 0: return None
    std = (sum((x - avg)**2 for x in values) / len(values)) ** 0.5
    return std / avg

def _trend_duration(bbp_series):
    if len(bbp_series) < 2: return 0
    direction = _sign(bbp_series[0] - bbp_series[1])
    if direction == 0: return 0
    count = 1
    for i in range(1, len(bbp_series) - 1):
        if _sign(bbp_series[i] - bbp_series[i+1]) == direction:
            count += 1
        else:
            break
    return int(direction * count)


# ---------------------------------------------------------------------------
# Shared scan-level features
# ---------------------------------------------------------------------------

def _get_breadth(scan_id):
    try:
        rows = execute_query("""
            SELECT a.bbp AS bbp_new, b.bbp AS bbp_old
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id
              AND b.scan_id = (SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1)
            WHERE a.scan_id = %s
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
        """, (scan_id, scan_id))

        if not rows: return None, None

        total   = len(rows)
        rising  = sum(1 for r in rows if r["bbp_new"] > r["bbp_old"] * 1.005)
        breadth = rising / total

        prev_rows = execute_query("""
            SELECT a.bbp AS bbp_new, b.bbp AS bbp_old
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id
              AND b.scan_id = (SELECT id FROM scans WHERE id < (
                  SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1
              ) ORDER BY id DESC LIMIT 1)
            WHERE a.scan_id = (SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1)
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
        """, (scan_id, scan_id))

        prev_breadth = None
        if prev_rows:
            pt = len(prev_rows)
            pr = sum(1 for r in prev_rows if r["bbp_new"] > r["bbp_old"] * 1.005)
            prev_breadth = pr / pt

        return breadth, _pct(breadth, prev_breadth)
    except Exception as e:
        logger.warning(f"Breadth failed: {e}")
        return None, None


def _get_event_features(scan_ts):
    try:
        rows = execute_query("""
            SELECT COUNT(*) AS scans_since
            FROM scans
            WHERE ts > (
                SELECT COALESCE(MAX(ts), '2000-01-01'::timestamptz)
                FROM events
                WHERE severity IN ('major','catastrophic') AND ts < %s::timestamptz
            )
            AND ts <= %s::timestamptz
        """, (scan_ts, scan_ts))
        scans_since = rows[0]["scans_since"] if rows else 9999
        return {"scans_since_event": scans_since,
                "is_post_event": 1 if scans_since <= POST_EVENT_WINDOW else 0}
    except Exception:
        return {"scans_since_event": 9999, "is_post_event": 0}


# ---------------------------------------------------------------------------
# Per-item feature computation
# ---------------------------------------------------------------------------

def _compute_item_features(item_id, scan_id, breadth, breadth_change,
                            event_feats, scan_ts):
    # Pull snapshot history (includes new columns)
    rows = execute_query("""
        SELECT sn.scan_id, sn.bbp, sn.gap_pct, sn.depth, sn.top_bid,
               sn.whale_hhi, sn.avg_order_age_hours, sn.tte_pressure,
               sn.implied_volume, sn.cancel_count, sn.fill_count,
               sn.requote_count
        FROM snapshots sn
        WHERE sn.item_id = %s
          AND sn.scan_id <= %s
          AND sn.bbp IS NOT NULL
        ORDER BY sn.scan_id DESC
        LIMIT %s
    """, (item_id, scan_id, CALIBRATION_WINDOW + FEATURE_MIN_HISTORY))

    if len(rows) < FEATURE_MIN_HISTORY or rows[0]["scan_id"] != scan_id:
        return None

    cur = rows[0]
    bbp = cur["bbp"]
    gap = cur["gap_pct"]
    dep = cur["depth"]
    top = cur["top_bid"]

    # BBP momentum
    bbp_series   = [r["bbp"] for r in rows if r["bbp"] is not None]
    bbp_change_1 = _pct(bbp, bbp_series[1] if len(bbp_series) > 1 else None)
    bbp_change_3 = _pct(bbp, bbp_series[3] if len(bbp_series) > 3 else None)
    bbp_change_6 = _pct(bbp, bbp_series[6] if len(bbp_series) > 6 else None)
    bbp_vol_6    = _volatility(bbp_series[:6])
    trend_dur    = _trend_duration(bbp_series)

    # Gap
    gap_series   = [r["gap_pct"] for r in rows if r.get("gap_pct") is not None]
    gap_change_1 = _pct(gap, gap_series[1] if len(gap_series) > 1 else None)
    gap_z        = _zscore(gap, gap_series[1:CALIBRATION_WINDOW+1]) if gap and len(gap_series) > 5 else None

    # Depth
    dep_series      = [r["depth"] for r in rows if r.get("depth") is not None]
    dep_1           = dep_series[1] if len(dep_series) > 1 else None
    dep_2           = dep_series[2] if len(dep_series) > 2 else None
    depth_change_1  = _pct(dep, dep_1)
    depth_change_2  = _pct(dep_1, dep_2) if dep_1 else None
    depth_accel     = (depth_change_1 - depth_change_2) if (depth_change_1 and depth_change_2) else None
    depth_z         = _zscore(dep, dep_series[1:CALIBRATION_WINDOW+1]) if dep and len(dep_series) > 5 else None

    # Whale
    top_series        = [r["top_bid"] for r in rows if r.get("top_bid")]
    top_bid_change    = _pct(top, top_series[1] if len(top_series) > 1 else None)
    top_bid_vs_bbp    = (top / bbp) if top and bbp else None
    hhi               = cur.get("whale_hhi")
    hhi_series        = [r["whale_hhi"] for r in rows if r.get("whale_hhi") is not None]
    whale_hhi_change  = _pct(hhi, hhi_series[1] if len(hhi_series) > 1 else None)

    # Order lifecycle (directly from current snapshot)
    avg_age     = cur.get("avg_order_age_hours")
    tte         = cur.get("tte_pressure")
    impl_vol    = cur.get("implied_volume") or 0
    cancels     = cur.get("cancel_count") or 0
    fills       = cur.get("fill_count") or 0
    requotes    = cur.get("requote_count") or 0

    # Divergence
    divergence = None
    if depth_change_1 is not None and bbp_change_1 is not None:
        divergence = _sign(depth_change_1) * _sign(bbp_change_1)

    # Cross-sectional
    item_vs_breadth = (bbp_change_1 - breadth) if (bbp_change_1 and breadth) else None

    # Time
    try:
        ts_dt       = datetime.fromisoformat(str(scan_ts).replace("Z", "+00:00"))
        hour_of_day = ts_dt.hour
        is_weekend  = 1 if ts_dt.weekday() >= 5 else 0
    except Exception:
        hour_of_day = None
        is_weekend  = None

    return {
        "scan_id":              scan_id,
        "item_id":              item_id,
        "bbp_change_1":         bbp_change_1,
        "bbp_change_3":         bbp_change_3,
        "bbp_change_6":         bbp_change_6,
        "bbp_volatility_6":     bbp_vol_6,
        "trend_duration":       trend_dur,
        "gap_pct":              gap,
        "gap_change_1":         gap_change_1,
        "gap_z":                gap_z,
        "depth":                dep,
        "depth_change_1":       depth_change_1,
        "depth_z":              depth_z,
        "depth_acceleration":   depth_accel,
        "top_bid_change_1":     top_bid_change,
        "top_bid_vs_bbp":       top_bid_vs_bbp,
        "whale_hhi":            hhi,
        "whale_hhi_change_1":   whale_hhi_change,
        "avg_order_age_hours":  avg_age,
        "tte_pressure":         tte,
        "implied_volume":       impl_vol,
        "cancel_count":         cancels,
        "fill_count":           fills,
        "requote_count":        requotes,
        "depth_bbp_divergence": divergence,
        "breadth":              breadth,
        "item_vs_breadth":      item_vs_breadth,
        "breadth_change_1":     breadth_change,
        "scans_since_event":    event_feats.get("scans_since_event", 9999),
        "is_post_event":        event_feats.get("is_post_event", 0),
        "hour_of_day":          hour_of_day,
        "is_weekend":           is_weekend,
        "label":                None,
    }


# ---------------------------------------------------------------------------
# Label backfill
# ---------------------------------------------------------------------------

def _backfill_labels(scan_id):
    rows = execute_query("""
        SELECT f.id, sn_prev.bbp AS bbp_old, sn_curr.bbp AS bbp_new
        FROM features f
        JOIN snapshots sn_prev
          ON sn_prev.item_id = f.item_id
          AND sn_prev.scan_id = (SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1)
        JOIN snapshots sn_curr
          ON sn_curr.item_id = f.item_id AND sn_curr.scan_id = %s
        WHERE f.scan_id = (SELECT id FROM scans WHERE id < %s ORDER BY id DESC LIMIT 1)
          AND f.label IS NULL
          AND sn_prev.bbp IS NOT NULL AND sn_curr.bbp IS NOT NULL
    """, (scan_id, scan_id, scan_id))

    if not rows: return

    updates = [((r["bbp_new"] - r["bbp_old"]) / r["bbp_old"], r["id"])
               for r in rows if r["bbp_old"] and r["bbp_old"] != 0]

    if updates:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.executemany("UPDATE features SET label = %s WHERE id = %s", updates)
            conn.commit()
        logger.info(f"Backfilled {len(updates)} labels")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extract_and_store(scan_id):
    _backfill_labels(scan_id)

    ts_rows = execute_query("SELECT ts FROM scans WHERE id = %s", (scan_id,))
    scan_ts = ts_rows[0]["ts"] if ts_rows else datetime.now(timezone.utc).isoformat()

    items = execute_query(
        "SELECT DISTINCT item_id FROM snapshots WHERE scan_id = %s", (scan_id,)
    )
    if not items: return

    breadth, breadth_change = _get_breadth(scan_id)
    event_feats             = _get_event_features(str(scan_ts))

    feature_rows = []
    skipped      = 0

    for row in items:
        feats = _compute_item_features(
            row["item_id"], scan_id,
            breadth, breadth_change,
            event_feats, scan_ts
        )
        if feats:
            feature_rows.append(feats)
        else:
            skipped += 1

    if not feature_rows:
        logger.info(f"No features for scan {scan_id}")
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO features (
                    scan_id, item_id,
                    bbp_change_1, bbp_change_3, bbp_change_6,
                    bbp_volatility_6, trend_duration,
                    gap_pct, gap_change_1, gap_z,
                    depth, depth_change_1, depth_z, depth_acceleration,
                    top_bid_change_1, top_bid_vs_bbp,
                    whale_hhi, whale_hhi_change_1,
                    avg_order_age_hours, tte_pressure,
                    implied_volume, cancel_count, fill_count,
                    requote_count,
                    depth_bbp_divergence,
                    breadth, item_vs_breadth, breadth_change_1,
                    scans_since_event, is_post_event,
                    hour_of_day, is_weekend, label
                ) VALUES (
                    %(scan_id)s, %(item_id)s,
                    %(bbp_change_1)s, %(bbp_change_3)s, %(bbp_change_6)s,
                    %(bbp_volatility_6)s, %(trend_duration)s,
                    %(gap_pct)s, %(gap_change_1)s, %(gap_z)s,
                    %(depth)s, %(depth_change_1)s, %(depth_z)s, %(depth_acceleration)s,
                    %(top_bid_change_1)s, %(top_bid_vs_bbp)s,
                    %(whale_hhi)s, %(whale_hhi_change_1)s,
                    %(avg_order_age_hours)s, %(tte_pressure)s,
                    %(implied_volume)s, %(cancel_count)s, %(fill_count)s,
                    %(requote_count)s,
                    %(depth_bbp_divergence)s,
                    %(breadth)s, %(item_vs_breadth)s, %(breadth_change_1)s,
                    %(scans_since_event)s, %(is_post_event)s,
                    %(hour_of_day)s, %(is_weekend)s, %(label)s
                )
            """, feature_rows)
        conn.commit()

    logger.info(f"Features: {len(feature_rows)} computed, {skipped} skipped for scan {scan_id}")


def get_labeled_count():
    rows = execute_query("SELECT COUNT(*) AS n FROM features WHERE label IS NOT NULL")
    return rows[0]["n"] if rows else 0


def get_training_data_summary():
    rows = execute_query("""
        SELECT COUNT(*) AS total_rows, COUNT(label) AS labeled_rows,
               COUNT(DISTINCT item_id) AS unique_items,
               COUNT(DISTINCT scan_id) AS scans_covered,
               AVG(CASE WHEN label IS NOT NULL THEN ABS(label) END) AS avg_abs_label,
               MIN(label) AS min_label, MAX(label) AS max_label
        FROM features
    """)
    return rows[0] if rows else {}