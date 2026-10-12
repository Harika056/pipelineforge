"""Command line: demo, batch, cdc, report.

  python -m pipelineforge.run demo            # generate data, run batches, apply CDC, print results
  python -m pipelineforge.run batch --oltp data/oltp.db --warehouse data/warehouse.db
  python -m pipelineforge.run cdc --events data/events.jsonl --warehouse data/warehouse.db
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3

from . import analytics
from .cdc import apply_file
from .generate import generate, mutate
from .pipeline import run_batch
from .schema import WAREHOUSE_DDL, connect


def _show(title: str, stats: dict) -> None:
    print(f"\n== {title}")
    print(json.dumps(stats, indent=2))


def demo(args) -> None:
    os.makedirs(args.dir, exist_ok=True)
    oltp, wh = os.path.join(args.dir, "oltp.db"), os.path.join(args.dir, "warehouse.db")
    for p in (oltp, wh, wh + "-wal", wh + "-shm", oltp + "-wal", oltp + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    info = generate(oltp, orders=args.orders, dirty_rate=args.dirty_rate)
    _show("generated synthetic source data (planted = known bad rows)", info)
    _show("batch 1 (full load)", run_batch(oltp, wh, args.engine))
    print("\nsimulating new orders / status updates / price changes in the source ...")
    mutate(oltp, at="2025-06-01T00:00:00")
    _show("batch 2 (incremental)", run_batch(oltp, wh, args.engine))
    _show("batch 3 (nothing new: only boundary rows are re-read, result unchanged)",
          run_batch(oltp, wh, args.engine))
    conn = sqlite3.connect(wh)
    print("\n== top customers by revenue (synthetic data)")
    print(analytics.run(conn, "top_customers").to_string(index=False))
    print("\n== quarantined rows by reason")
    for r in conn.execute("SELECT source, reason, COUNT(*) FROM quarantine GROUP BY 1,2 ORDER BY 3 DESC"):
        print("  ", r)
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo")
    d.add_argument("--dir", default="data"); d.add_argument("--orders", type=int, default=2000)
    d.add_argument("--dirty-rate", type=float, default=0.02)
    d.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    b = sub.add_parser("batch")
    b.add_argument("--oltp", default="data/oltp.db"); b.add_argument("--warehouse", default="data/warehouse.db")
    b.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    c = sub.add_parser("cdc")
    c.add_argument("--events", required=True); c.add_argument("--warehouse", default="data/warehouse.db")
    c.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    a = ap.parse_args()
    if a.cmd == "demo":
        demo(a)
    elif a.cmd == "batch":
        _show("batch", run_batch(a.oltp, a.warehouse, a.engine))
    else:
        conn = connect(a.warehouse, WAREHOUSE_DDL)
        _show("cdc", apply_file(conn, a.events, a.engine))


if __name__ == "__main__":
    main()
