"""One incremental batch run: extract -> validate -> load (single transaction) -> reconcile."""
from __future__ import annotations

import time
import uuid

from .extract import extract, get_watermarks
from .load import quarantine, rebuild_fact, upsert
from .quality import validate_customers, validate_items, validate_orders, validate_products
from .schema import OLTP_DDL, WAREHOUSE_DDL, connect


def _ids(conn, sql):
    return {r[0] for r in conn.execute(sql)}


def run_batch(oltp_path: str, wh_path: str, engine: str = "pandas") -> dict:
    run_id = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    oltp = connect(oltp_path, OLTP_DDL)
    wh = connect(wh_path, WAREHOUSE_DDL)
    try:
        data = extract(oltp, get_watermarks(wh))
        t_extract = time.perf_counter() - t0

        wh.execute("BEGIN")
        try:
            rejects, results = [], []
            cust, r, res = validate_customers(data["customers"]); rejects += r; results += res
            prod, r, res = validate_products(data["products"]); rejects += r; results += res
            known_c = _ids(wh, "SELECT customer_id FROM dim_customer") | set(cust["customer_id"])
            known_p = _ids(wh, "SELECT product_id FROM dim_product") | set(prod["product_id"])
            orders, r, res = validate_orders(data["orders"], known_c); rejects += r; results += res
            known_o = _ids(wh, "SELECT order_id FROM stg_orders") | set(orders["order_id"])
            items, r, res = validate_items(data["order_items"], known_o, known_p); rejects += r; results += res

            upsert(wh, "dim_customer", cust)
            upsert(wh, "dim_product", prod)
            upsert(wh, "stg_orders", orders)
            upsert(wh, "stg_items", items)
            quarantine(wh, run_id, rejects)
            wh.executemany("INSERT INTO dq_report (run_id, source, rule, failed, total) VALUES (?,?,?,?,?)",
                           [(run_id, x.source, x.rule, x.failed, x.total) for x in results])
            fact_stats = rebuild_fact(wh, set(data["orders"]["order_id"]), engine)
            wh.executemany("INSERT INTO etl_state (source, watermark) VALUES (?,?) "
                           "ON CONFLICT(source) DO UPDATE SET watermark=excluded.watermark",
                           list(data["watermarks"].items()))
            wh.execute("COMMIT")
        except BaseException:
            wh.execute("ROLLBACK")
            raise
        reasons: dict = {}
        for x in rejects:
            key = f"{x['source']}.{x['reason']}"
            reasons[key] = reasons.get(key, 0) + 1
        return {
            "run_id": run_id, "engine": engine,
            "extracted": {k: len(data[k]) for k in ("customers", "products", "orders", "order_items")},
            "quarantined": reasons, "quarantined_total": len(rejects),
            "fact": fact_stats,
            "seconds": {"extract": round(t_extract, 4), "total": round(time.perf_counter() - t0, 4)},
        }
    finally:
        oltp.close()
        wh.close()
