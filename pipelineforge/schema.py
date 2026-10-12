"""SQL schemas and connection helper. OLTP = the 'source' database, WAREHOUSE = the analytics database."""
from __future__ import annotations

import sqlite3

VALID_STATUS = ("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")

OLTP_DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY, name TEXT, category TEXT, unit_price REAL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, order_ts TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS order_items (
    item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER,
    unit_price REAL, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_customers_upd ON customers(updated_at);
CREATE INDEX IF NOT EXISTS ix_products_upd ON products(updated_at);
CREATE INDEX IF NOT EXISTS ix_orders_upd ON orders(updated_at);
CREATE INDEX IF NOT EXISTS ix_items_upd ON order_items(updated_at);
CREATE INDEX IF NOT EXISTS ix_items_order ON order_items(order_id);
"""

WAREHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY, name TEXT, category TEXT, unit_price REAL, updated_at TEXT);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, date TEXT, year INTEGER, month INTEGER, day INTEGER, weekday INTEGER);
-- staging copies of validated source rows; the fact table is always rebuilt from these
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, order_ts TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS stg_items (
    item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER,
    unit_price REAL, updated_at TEXT);
CREATE INDEX IF NOT EXISTS ix_stg_items_order ON stg_items(order_id);
CREATE TABLE IF NOT EXISTS fact_sales (
    item_id INTEGER PRIMARY KEY, order_id INTEGER, customer_id INTEGER, product_id INTEGER,
    date_key INTEGER, quantity INTEGER, unit_price REAL, revenue REAL, order_status TEXT);
CREATE INDEX IF NOT EXISTS ix_fact_order ON fact_sales(order_id);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, source TEXT, row_key TEXT, reason TEXT, payload TEXT,
    UNIQUE(source, row_key, reason));
CREATE TABLE IF NOT EXISTS etl_state (source TEXT PRIMARY KEY, watermark TEXT);
CREATE TABLE IF NOT EXISTS dq_report (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, source TEXT, rule TEXT, failed INTEGER, total INTEGER);
-- last applied change-event timestamp per source row (makes CDC replay and out-of-order safe)
CREATE TABLE IF NOT EXISTS cdc_versions (
    tbl TEXT, pk INTEGER, ts_ms INTEGER, deleted INTEGER, PRIMARY KEY (tbl, pk));
"""


def connect(path: str, ddl: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)  # we manage transactions explicitly
    conn.execute("PRAGMA journal_mode=WAL")
    if ddl:
        conn.executescript(ddl)
    return conn
