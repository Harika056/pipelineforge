"""Incremental extraction using an `updated_at` high-water mark per source table.

Rows with updated_at >= watermark are re-read on the next run (>=, not >), so rows that share the
boundary timestamp are never missed. Loading is an upsert, so re-reading them is harmless.
"""
from __future__ import annotations

import sqlite3

import pandas as pd

TABLES = {"customers": "customer_id", "products": "product_id", "orders": "order_id",
          "order_items": "item_id"}


def get_watermarks(wh: sqlite3.Connection) -> dict:
    rows = dict(wh.execute("SELECT source, watermark FROM etl_state").fetchall())
    return {t: rows.get(t, "") for t in TABLES}


def extract(oltp: sqlite3.Connection, watermarks: dict) -> dict:
    """Read all changes in ONE read transaction (consistent snapshot).

    Returns {'customers','products','orders','order_items': DataFrame, 'watermarks': {...}}.
    An order is 'affected' if the order row OR any of its items changed, and then the order and ALL
    its items are returned, so the fact table can be rebuilt for the whole order.
    """
    oltp.execute("BEGIN")
    try:
        out = {}
        new_wm = {}
        for t in ("customers", "products"):
            out[t] = pd.read_sql_query(f"SELECT * FROM {t} WHERE updated_at >= ? ORDER BY updated_at",
                                       oltp, params=(watermarks[t],))
        ids = [r[0] for r in oltp.execute(
            "SELECT order_id FROM orders WHERE updated_at >= ? "
            "UNION SELECT order_id FROM order_items WHERE updated_at >= ?",
            (watermarks["orders"], watermarks["order_items"]))]
        oltp.execute("CREATE TEMP TABLE IF NOT EXISTS _aff(order_id INTEGER PRIMARY KEY)")
        oltp.execute("DELETE FROM _aff")
        oltp.executemany("INSERT OR IGNORE INTO _aff VALUES (?)", [(i,) for i in ids])
        out["orders"] = pd.read_sql_query(
            "SELECT o.* FROM orders o JOIN _aff a ON a.order_id=o.order_id ORDER BY o.order_id", oltp)
        out["order_items"] = pd.read_sql_query(
            "SELECT i.* FROM order_items i JOIN _aff a ON a.order_id=i.order_id ORDER BY i.item_id", oltp)
        for t in TABLES:
            m = oltp.execute(f"SELECT MAX(updated_at) FROM {t}").fetchone()[0]
            new_wm[t] = m or watermarks[t]
        out["watermarks"] = new_wm
        return out
    finally:
        oltp.execute("COMMIT")
