"""Measure end-to-end batch throughput on synthetic data.  python -m scripts.benchmark --orders 20000"""
from __future__ import annotations

import argparse
import os
import tempfile
import time

from pipelineforge.generate import generate
from pipelineforge.pipeline import run_batch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=20000)
    ap.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    times = []
    for _ in range(a.repeats):
        with tempfile.TemporaryDirectory() as d:
            oltp, wh = os.path.join(d, "o.db"), os.path.join(d, "w.db")
            info = generate(oltp, customers=2000, products=200, orders=a.orders, dirty_rate=0.01)
            t = time.perf_counter()
            stats = run_batch(oltp, wh, a.engine)
            times.append(time.perf_counter() - t)
    times.sort()
    med = times[len(times) // 2]
    rows = info["items"]
    print(f"engine={a.engine} orders={a.orders} items={rows} runs={a.repeats}")
    print(f"batch seconds (min/median/max): {times[0]:.2f} / {med:.2f} / {times[-1]:.2f}")
    print(f"median throughput: {rows / med:,.0f} source item-rows/sec (synthetic data, single machine)")
    print(f"fact rows written: {stats['fact']['fact_rows']}, quarantined: {stats['quarantined_total']}")


if __name__ == "__main__":
    main()
