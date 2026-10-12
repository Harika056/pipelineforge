"""Warehouse writes: upserts, quarantine, and the fact rebuild (shared by the batch and CDC paths)."""
from __future__ import annotations

import sqlite3

import pandas as pd

from .transform import build_dim_date, build_fact

COLUMNS = {
    "dim_customer": ["customer_id", "name", "email", "country", "updated_at"],
    "dim_product": ["product_id", "name", "category", "unit_price", "updated_at"],
    "stg_orders": ["order_id", "customer_id", "status", "order_ts", "updated_at"],
    "stg_items": ["item_id", "order_id", "product_id", "quantity", "unit_price", "updated_at"],
    "dim_date": ["date_key", "date", "year", "month", "day", "weekday"],
    "fact_sales": ["item_id", "order_id", "customer_id", "product_id", "date_key", "quantity",
                   "unit_price", "revenue", "order_status"],
}


def _records(df: pd.DataFrame, cols: list) -> list:
    return df[cols].astype(object).where(df[cols].notna(), None).values.tolist()


def upsert(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    cols = COLUMNS[table]
    pk = cols[0]
    sets = ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
    sql = (f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
           f"ON CONFLICT({pk}) DO UPDATE SET {sets}")
    conn.executemany(sql, _records(df, cols))
    return len(df)


def quarantine(conn: sqlite3.Connection, run_id: str, rejects: list) -> int:
    conn.executemany("INSERT OR REPLACE INTO quarantine (run_id, source, row_key, reason, payload) VALUES (?,?,?,?,?)",
                     [(run_id, r["source"], r["row_key"], r["reason"], r["payload"]) for r in rejects])
    return len(rejects)


def _id_table(conn: sqlite3.Connection, order_ids) -> None:
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _ids(order_id INTEGER PRIMARY KEY)")
    conn.execute("DELETE FROM _ids")
    conn.executemany("INSERT OR IGNORE INTO _ids VALUES (?)", [(int(i),) for i in order_ids])


def rebuild_fact(conn: sqlite3.Connection, order_ids, engine: str = "pandas") -> dict:
    """Recompute fact rows for the given orders from staging + dimensions.

    Items whose order, customer or product is missing from the warehouse are left out of the fact
    (counted as `unresolved`). Must be called inside a transaction.
    """
    order_ids = sorted({int(i) for i in order_ids})
    if not order_ids:
        return {"orders": 0, "fact_rows": 0, "unresolved": 0}
    _id_table(conn, order_ids)
    joined = pd.read_sql_query(
        """SELECT i.item_id, i.order_id, o.customer_id, i.product_id, i.quantity, i.unit_price,
                  o.status, o.order_ts
           FROM stg_items i
           JOIN _ids x ON x.order_id = i.order_id
           JOIN stg_orders o ON o.order_id = i.order_id
           JOIN dim_customer c ON c.customer_id = o.customer_id
           JOIN dim_product p ON p.product_id = i.product_id""", conn)
    total_items = conn.execute(
        "SELECT COUNT(*) FROM stg_items i JOIN _ids x ON x.order_id=i.order_id").fetchone()[0]
    fact = build_fact(joined, engine)
    conn.execute("DELETE FROM fact_sales WHERE order_id IN (SELECT order_id FROM _ids)")
    upsert(conn, "dim_date", build_dim_date(fact))
    upsert(conn, "fact_sales", fact)
    return {"orders": len(order_ids), "fact_rows": len(fact), "unresolved": total_items - len(fact)}
