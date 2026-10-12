"""Synthetic OLTP data generator (fictional customers/products/orders).

Everything produced here is synthetic. `dirty_rate` injects known-bad rows so the data-quality
checks can be tested against a ground truth of how many bad rows were planted.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from .schema import OLTP_DDL, VALID_STATUS, connect

COUNTRIES = ["IN", "US", "DE", "FR", "GB", "SG", "AU", "BR"]
CATEGORIES = ["books", "electronics", "home", "toys", "sports", "grocery"]
FMT = "%Y-%m-%dT%H:%M:%S"


def _ts(dt: datetime) -> str:
    return dt.strftime(FMT)


def generate(path: str, customers: int = 200, products: int = 50, orders: int = 1000,
             seed: int = 7, dirty_rate: float = 0.0, start: str = "2025-01-01", days: int = 90) -> dict:
    """Create the OLTP database at `path`. Returns counts, including how many bad rows were planted."""
    rng = random.Random(seed)
    conn = connect(path, OLTP_DDL)
    t0 = datetime.fromisoformat(start)
    planted = {"null_email": 0, "bad_status": 0, "bad_quantity": 0, "orphan_product": 0}

    conn.execute("BEGIN")
    cust_rows = []
    for cid in range(1, customers + 1):
        email = f"user{cid}@example.com"
        if rng.random() < dirty_rate:
            email, planted["null_email"] = None, planted["null_email"] + 1
        cust_rows.append((cid, f"Customer {cid}", email, rng.choice(COUNTRIES), _ts(t0)))
    conn.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", cust_rows)

    prices = {}
    prod_rows = []
    for pid in range(1, products + 1):
        prices[pid] = round(rng.uniform(3, 250), 2)
        prod_rows.append((pid, f"Product {pid}", rng.choice(CATEGORIES), prices[pid], _ts(t0)))
    conn.executemany("INSERT INTO products VALUES (?,?,?,?,?)", prod_rows)

    item_id = 0
    order_rows, item_rows = [], []
    for oid in range(1, orders + 1):
        ts = t0 + timedelta(seconds=rng.randrange(days * 86400))
        status = rng.choice(VALID_STATUS)
        if rng.random() < dirty_rate:
            status, planted["bad_status"] = "UNKNOWN", planted["bad_status"] + 1
        order_rows.append((oid, rng.randint(1, customers), status, _ts(ts), _ts(ts)))
        for _ in range(rng.randint(1, 4)):
            item_id += 1
            pid, qty = rng.randint(1, products), rng.randint(1, 5)
            if rng.random() < dirty_rate:
                qty, planted["bad_quantity"] = -qty, planted["bad_quantity"] + 1
            elif rng.random() < dirty_rate:
                pid, planted["orphan_product"] = 999999, planted["orphan_product"] + 1
            item_rows.append((item_id, oid, pid, qty, prices.get(pid, 10.0), _ts(ts)))
    conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", order_rows)
    conn.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", item_rows)
    conn.execute("COMMIT")
    conn.close()
    return {"customers": customers, "products": products, "orders": orders, "items": item_id,
            "planted": planted}


def mutate(path: str, at: str, seed: int = 11, new_orders: int = 50, status_updates: int = 30,
           price_changes: int = 5) -> dict:
    """Simulate later activity in the OLTP db, stamping every change with updated_at=`at`."""
    rng = random.Random(seed)
    conn = connect(path, OLTP_DDL)
    conn.execute("BEGIN")
    max_order = conn.execute("SELECT COALESCE(MAX(order_id),0) FROM orders").fetchone()[0]
    max_item = conn.execute("SELECT COALESCE(MAX(item_id),0) FROM order_items").fetchone()[0]
    n_cust = conn.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
    n_prod = conn.execute("SELECT MAX(product_id) FROM products").fetchone()[0]
    for oid in rng.sample(range(1, max_order + 1), min(status_updates, max_order)):
        conn.execute("UPDATE orders SET status=?, updated_at=? WHERE order_id=?",
                     (rng.choice(VALID_STATUS), at, oid))
    for pid in rng.sample(range(1, n_prod + 1), min(price_changes, n_prod)):
        conn.execute("UPDATE products SET unit_price=ROUND(unit_price*1.1,2), updated_at=? WHERE product_id=?",
                     (at, pid))
    for k in range(1, new_orders + 1):
        oid = max_order + k
        conn.execute("INSERT INTO orders VALUES (?,?,?,?,?)",
                     (oid, rng.randint(1, n_cust), "PLACED", at, at))
        for _ in range(rng.randint(1, 3)):
            max_item += 1
            pid = rng.randint(1, n_prod)
            price = conn.execute("SELECT unit_price FROM products WHERE product_id=?", (pid,)).fetchone()[0]
            conn.execute("INSERT INTO order_items VALUES (?,?,?,?,?,?)",
                         (max_item, oid, pid, rng.randint(1, 5), price, at))
    conn.execute("COMMIT")
    conn.close()
    return {"new_orders": new_orders, "status_updates": status_updates, "price_changes": price_changes}
