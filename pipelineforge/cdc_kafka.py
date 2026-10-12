"""Kafka consumer that feeds Debezium events into apply_events.   *** NOT TESTED against a real broker ***

    python -m pipelineforge.cdc_kafka --bootstrap localhost:9092 --topic shop.public.orders \
        --topic shop.public.order_items --warehouse data/warehouse.db

Requires `pip install aiokafka`. The apply logic itself (pipelineforge.cdc) is unit-tested; this file
is only the thin transport layer.
"""
from __future__ import annotations

import argparse
import asyncio

from .cdc import apply_events
from .schema import WAREHOUSE_DDL, connect


async def run(bootstrap: str, topics: list, warehouse: str, batch_size: int, batch_ms: int) -> None:
    from aiokafka import AIOKafkaConsumer
    wh = connect(warehouse, WAREHOUSE_DDL)
    consumer = AIOKafkaConsumer(*topics, bootstrap_servers=bootstrap, group_id="pipelineforge",
                                enable_auto_commit=False, auto_offset_reset="earliest")
    await consumer.start()
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=batch_ms, max_records=batch_size)
            msgs = [m.value for recs in batches.values() for m in recs if m.value]  # skip tombstones
            if not msgs:
                continue
            print(apply_events(wh, msgs))
            await consumer.commit()  # commit offsets only after the warehouse transaction succeeded
    finally:
        await consumer.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", action="append", required=True)
    ap.add_argument("--warehouse", default="data/warehouse.db")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--batch-ms", type=int, default=1000)
    a = ap.parse_args()
    asyncio.run(run(a.bootstrap, a.topic, a.warehouse, a.batch_size, a.batch_ms))


if __name__ == "__main__":
    main()
