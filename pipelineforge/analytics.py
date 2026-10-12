"""Example analytical queries on the warehouse."""
from __future__ import annotations

import sqlite3

import pandas as pd

QUERIES = {
    "revenue_by_category_month": """
        SELECT d.year, d.month, p.category, ROUND(SUM(f.revenue), 2) AS revenue, COUNT(*) AS items
        FROM fact_sales f JOIN dim_date d ON d.date_key=f.date_key JOIN dim_product p ON p.product_id=f.product_id
        WHERE f.order_status <> 'CANCELLED'
        GROUP BY d.year, d.month, p.category ORDER BY d.year, d.month, revenue DESC""",
    "top_customers": """
        SELECT c.customer_id, c.name, c.country, ROUND(SUM(f.revenue), 2) AS revenue
        FROM fact_sales f JOIN dim_customer c ON c.customer_id=f.customer_id
        WHERE f.order_status <> 'CANCELLED' GROUP BY c.customer_id ORDER BY revenue DESC LIMIT 10""",
}


def run(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    return pd.read_sql_query(QUERIES[name], conn)
