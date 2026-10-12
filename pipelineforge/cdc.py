"""Apply Debezium-style change events to the warehouse.

Event format: the standard Debezium envelope fields are used (`op` c/u/d/r, `before`, `after`,
`source.table`, `ts_ms`), with or without the outer {"schema":..., "payload":...} wrapper.
Column values are plain JSON (our own simplified encoding, e.g. ISO timestamps), not Debezium's
logical types; a real Debezium connector would need a small value-conversion step.

Safe to replay and to receive out of order: each row remembers the newest ts_ms applied to it.
"""
from __future__ import annotations

import json
import sqlite3
import uuid

import pandas as pd

from .load import quarantine, rebuild_fact, upsert
from .quality import validate_customers, validate_items, validate_orders, validate_products

TABLE_MAP = {  # source table -> (warehouse table, pk column)
    "customers": ("dim_customer", "customer_id"),
    "products": ("dim_product", "product_id"),
    "orders": ("stg_orders", "order_id"),
    "order_items": ("stg_items", "item_id"),
}


def make_event(table: str, op: str, before: dict | None, after: dict | None, ts_ms: int) -> dict:
    return {"payload": {"op": op, "before": before, "after": after, "ts_ms": ts_ms,
                        "source": {"table": table}}}


def parse_event(raw) -> dict:
    ev = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    p = ev.get("payload", ev)
    table = p["source"]["table"]
    if table not in TABLE_MAP:
        raise ValueError(f"unsupported table {table!r}")
    op = p["op"]
    row = p.get("before") if op == "d" else p.get("after")
    if row is None:
        raise ValueError("event has no row image")
    return {"table": table, "op": op, "row": row, "ts_ms": int(p["ts_ms"])}


def _rows_df(table: str, rows: list) -> pd.DataFrame:
    wh_table, _ = TABLE_MAP[table]
    from .load import COLUMNS
    cols = COLUMNS[wh_table]
    return pd.DataFrame([{c: r.get(c) for c in cols} for r in rows], columns=cols)


def apply_events(wh: sqlite3.Connection, raw_events, engine: str = "pandas") -> dict:
    run_id = uuid.uuid4().hex[:8]
    events = [parse_event(e) for e in raw_events]
    stats = {"received": len(events), "stale_skipped": 0, "applied": 0, "deleted": 0, "quarantined": 0}

    # keep only the newest event per (table, pk); later position wins ties
    latest: dict = {}
    for ev in events:
        pk = int(ev["row"][TABLE_MAP[ev["table"]][1]])
        key = (ev["table"], pk)
        if key not in latest or ev["ts_ms"] >= latest[key]["ts_ms"]:
            latest[key] = ev

    wh.execute("BEGIN")
    try:
        upserts: dict = {t: [] for t in TABLE_MAP}
        deletes: dict = {t: [] for t in TABLE_MAP}
        for (table, pk), ev in latest.items():
            seen = wh.execute("SELECT ts_ms FROM cdc_versions WHERE tbl=? AND pk=?", (table, pk)).fetchone()
            if seen and ev["ts_ms"] < seen[0]:
                stats["stale_skipped"] += 1
                continue
            (deletes if ev["op"] == "d" else upserts)[table].append(ev)

        affected: set = set()
        # deletes first
        for table, evs in deletes.items():
            wh_table, pkc = TABLE_MAP[table]
            for ev in evs:
                pk = int(ev["row"][pkc])
                if table == "order_items":
                    o = wh.execute("SELECT order_id FROM stg_items WHERE item_id=?", (pk,)).fetchone()
                    if o: affected.add(o[0])
                elif table == "orders":
                    affected.add(pk)
                elif table == "customers":
                    affected |= {r[0] for r in wh.execute(
                        "SELECT order_id FROM stg_orders WHERE customer_id=?", (pk,))}
                elif table == "products":
                    affected |= {r[0] for r in wh.execute(
                        "SELECT order_id FROM stg_items WHERE product_id=?", (pk,))}
                wh.execute(f"DELETE FROM {wh_table} WHERE {pkc}=?", (pk,))
                wh.execute("INSERT INTO cdc_versions VALUES (?,?,?,1) ON CONFLICT(tbl,pk) DO UPDATE "
                           "SET ts_ms=excluded.ts_ms, deleted=1", (table, pk, ev["ts_ms"]))
                stats["deleted"] += 1

        # upserts: validate with the same rules as the batch path
        rejects: list = []
        cust, r, _ = validate_customers(_rows_df("customers", [e["row"] for e in upserts["customers"]])); rejects += r
        prod, r, _ = validate_products(_rows_df("products", [e["row"] for e in upserts["products"]])); rejects += r
        known_c = {r[0] for r in wh.execute("SELECT customer_id FROM dim_customer")} | set(cust["customer_id"])
        known_p = {r[0] for r in wh.execute("SELECT product_id FROM dim_product")} | set(prod["product_id"])
        orders, r, _ = validate_orders(_rows_df("orders", [e["row"] for e in upserts["orders"]]), known_c); rejects += r
        known_o = {r[0] for r in wh.execute("SELECT order_id FROM stg_orders")} | set(orders["order_id"])
        items, r, _ = validate_items(_rows_df("order_items", [e["row"] for e in upserts["order_items"]]),
                                     known_o, known_p); rejects += r
        upsert(wh, "dim_customer", cust); upsert(wh, "dim_product", prod)
        upsert(wh, "stg_orders", orders); upsert(wh, "stg_items", items)
        quarantine(wh, run_id, rejects)
        stats["quarantined"] = len(rejects)

        accepted = {"customers": set(cust["customer_id"]), "products": set(prod["product_id"]),
                    "orders": set(orders["order_id"]), "order_items": set(items["item_id"])}
        for table, evs in upserts.items():
            pkc = TABLE_MAP[table][1]
            for ev in evs:
                pk = int(ev["row"][pkc])
                if pk not in accepted[table]:
                    continue
                wh.execute("INSERT INTO cdc_versions VALUES (?,?,?,0) ON CONFLICT(tbl,pk) DO UPDATE "
                           "SET ts_ms=excluded.ts_ms, deleted=0", (table, pk, ev["ts_ms"]))
                stats["applied"] += 1
        affected |= set(int(x) for x in orders["order_id"]) | set(int(x) for x in items["order_id"])
        stats["fact"] = rebuild_fact(wh, affected, engine)
        wh.execute("COMMIT")
    except BaseException:
        wh.execute("ROLLBACK")
        raise
    return stats


def apply_file(wh: sqlite3.Connection, path: str, engine: str = "pandas") -> dict:
    with open(path, encoding="utf-8") as f:
        return apply_events(wh, [line for line in f if line.strip()], engine)
