import json
import sqlite3

import pytest

from pipelineforge.cdc import apply_events, make_event, parse_event
from pipelineforge.pipeline import run_batch
from pipelineforge.schema import WAREHOUSE_DDL, connect
from tests.conftest import wh_rows

Q_FACT = "SELECT * FROM fact_sales ORDER BY item_id"


def _item(item_id, order_id, product_id=1, qty=2, price=10.0, ts="2031-03-01T00:00:00"):
    return {"item_id": item_id, "order_id": order_id, "product_id": product_id, "quantity": qty,
            "unit_price": price, "updated_at": ts}


def _order(order_id, customer_id=1, status="PLACED", ts="2031-03-01T00:00:00"):
    return {"order_id": order_id, "customer_id": customer_id, "status": status, "order_ts": ts,
            "updated_at": ts}


@pytest.fixture
def loaded(clean_src):
    (oltp, wh), info = clean_src
    run_batch(oltp, wh)
    return oltp, wh, info


def _conn(path):
    return connect(path, WAREHOUSE_DDL)


def test_parse_event_with_and_without_wrapper_and_delete_uses_before():
    ev = make_event("orders", "u", None, _order(1), 5)
    assert parse_event(json.dumps(ev))["row"]["order_id"] == 1
    assert parse_event(ev["payload"])["ts_ms"] == 5
    d = make_event("order_items", "d", _item(7, 1), None, 9)
    assert parse_event(d)["row"]["item_id"] == 7 and parse_event(d)["op"] == "d"


def test_parse_event_rejects_unknown_table_and_missing_row():
    with pytest.raises(ValueError):
        parse_event(make_event("secrets", "c", None, {"x": 1}, 1))
    with pytest.raises(ValueError):
        parse_event(make_event("orders", "c", None, None, 1))


def test_cdc_state_equals_batch_state_after_same_changes(loaded, tmp_path):
    oltp, wh, _ = loaded
    o = sqlite3.connect(oltp).execute("SELECT order_id, customer_id, status, order_ts, updated_at "
                                      "FROM orders WHERE order_id=1").fetchone()
    before = dict(zip(["order_id", "customer_id", "status", "order_ts", "updated_at"], o))
    after = dict(before, status="CANCELLED", updated_at="2031-03-01T00:00:00")   # full row image, one field changed
    events = [
        make_event("orders", "u", before, after, 100),
        make_event("orders", "c", None, _order(1000), 101),
        make_event("order_items", "c", None, _item(5000, 1000, product_id=2, qty=3, price=7.5), 102),
        make_event("order_items", "c", None, _item(5001, 1000, product_id=3, qty=1, price=20.0), 103),
    ]
    conn = _conn(wh)
    stats = apply_events(conn, events)
    assert stats["quarantined"] == 0 and stats["applied"] == 4

    # same changes applied to the source, then a from-scratch batch load into a new warehouse
    src = sqlite3.connect(oltp)
    src.execute("UPDATE orders SET status='CANCELLED', updated_at='2031-03-01T00:00:00' WHERE order_id=1")
    src.execute("INSERT INTO orders VALUES (1000,1,'PLACED','2031-03-01T00:00:00','2031-03-01T00:00:00')")
    src.execute("INSERT INTO order_items VALUES (5000,1000,2,3,7.5,'2031-03-01T00:00:00')")
    src.execute("INSERT INTO order_items VALUES (5001,1000,3,1,20.0,'2031-03-01T00:00:00')")
    src.commit()
    fresh = str(tmp_path / "fresh.db")
    run_batch(oltp, fresh)
    assert wh_rows(wh, Q_FACT) == wh_rows(fresh, Q_FACT)


def test_replaying_the_same_events_is_idempotent(loaded):
    _, wh, _ = loaded
    events = [make_event("orders", "u", None, _order(2, status="SHIPPED"), 50),
              make_event("order_items", "c", None, _item(6000, 2, qty=4), 51)]
    conn = _conn(wh)
    apply_events(conn, events)
    first = wh_rows(wh, Q_FACT)
    apply_events(conn, events)
    assert wh_rows(wh, Q_FACT) == first


def test_out_of_order_older_event_is_skipped(loaded):
    _, wh, _ = loaded
    conn = _conn(wh)
    apply_events(conn, [make_event("orders", "u", None, _order(3, status="DELIVERED"), 200)])
    stats = apply_events(conn, [make_event("orders", "u", None, _order(3, status="PAID"), 100)])
    assert stats["stale_skipped"] == 1
    assert wh_rows(wh, "SELECT status FROM stg_orders WHERE order_id=3") == [("DELIVERED",)]
    assert {r[0] for r in wh_rows(wh, "SELECT order_status FROM fact_sales WHERE order_id=3")} == {"DELIVERED"}


def test_only_newest_event_per_row_in_one_batch_wins(loaded):
    _, wh, _ = loaded
    conn = _conn(wh)
    apply_events(conn, [make_event("orders", "u", None, _order(4, status="PAID"), 10),
                        make_event("orders", "u", None, _order(4, status="SHIPPED"), 20),
                        make_event("orders", "u", None, _order(4, status="CANCELLED"), 15)])
    assert wh_rows(wh, "SELECT status FROM stg_orders WHERE order_id=4") == [("SHIPPED",)]


def test_invalid_change_is_quarantined_and_not_loaded(loaded):
    _, wh, _ = loaded
    before = wh_rows(wh, "SELECT COUNT(*) FROM fact_sales")[0][0]
    conn = _conn(wh)
    stats = apply_events(conn, [make_event("orders", "c", None, _order(2000), 1),
                                make_event("order_items", "c", None, _item(7000, 2000, qty=-5), 2)])
    assert stats["quarantined"] == 1
    assert wh_rows(wh, "SELECT reason FROM quarantine WHERE source='order_items' AND row_key='7000'") \
        == [("quantity_positive",)]
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales")[0][0] == before


def test_delete_item_and_delete_order_remove_fact_rows(loaded):
    _, wh, _ = loaded
    order_id, item_id = wh_rows(wh, "SELECT order_id, item_id FROM stg_items ORDER BY item_id LIMIT 1")[0]
    conn = _conn(wh)
    apply_events(conn, [make_event("order_items", "d", _item(item_id, order_id), None, 300)])
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales WHERE item_id=?", (item_id,))[0][0] == 0
    assert wh_rows(wh, "SELECT COUNT(*) FROM stg_items WHERE item_id=?", (item_id,))[0][0] == 0

    other = wh_rows(wh, "SELECT order_id FROM stg_orders WHERE order_id<>? ORDER BY order_id LIMIT 1", (order_id,))[0][0]
    apply_events(conn, [make_event("orders", "d", _order(other), None, 301)])
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales WHERE order_id=?", (other,))[0][0] == 0


def test_a_failure_rolls_back_the_whole_cdc_batch(loaded, monkeypatch):
    _, wh, _ = loaded
    from pipelineforge import cdc

    def boom(*a, **k):
        raise RuntimeError("crash")
    monkeypatch.setattr(cdc, "rebuild_fact", boom)
    conn = _conn(wh)
    with pytest.raises(RuntimeError):
        apply_events(conn, [make_event("orders", "u", None, _order(5, status="PAID"), 400)])
    assert wh_rows(wh, "SELECT COUNT(*) FROM cdc_versions")[0][0] == 0
