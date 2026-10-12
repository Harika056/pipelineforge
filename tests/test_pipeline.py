import sqlite3

import pytest

from pipelineforge import pipeline
from pipelineforge.generate import generate, mutate
from pipelineforge.pipeline import run_batch
from tests.conftest import wh_rows


def test_full_load_matches_independent_sql_on_clean_data(clean_src):
    (oltp, wh), info = clean_src
    stats = run_batch(oltp, wh)
    assert stats["quarantined_total"] == 0
    assert stats["fact"]["unresolved"] == 0
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales")[0][0] == info["items"]
    # independent computation straight from the source database
    src = sqlite3.connect(oltp)
    expected = src.execute("SELECT ROUND(SUM(quantity*unit_price),2) FROM order_items").fetchone()[0]
    got = wh_rows(wh, "SELECT ROUND(SUM(revenue),2) FROM fact_sales")[0][0]
    assert got == pytest.approx(expected, abs=0.05)


def test_every_planted_bad_row_is_quarantined_and_none_leak_into_the_fact(tmp_path):
    oltp, wh = str(tmp_path / "o.db"), str(tmp_path / "w.db")
    info = generate(oltp, customers=100, products=30, orders=1500, dirty_rate=0.03, seed=5)
    stats = run_batch(oltp, wh)
    q = stats["quarantined"]
    p = info["planted"]
    assert q.get("customers.email_valid", 0) == p["null_email"]
    assert q.get("orders.status_valid", 0) == p["bad_status"]
    assert q.get("order_items.quantity_positive", 0) + q.get("order_items.order_exists", 0) \
        + q.get("order_items.product_exists", 0) >= p["bad_quantity"]
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales WHERE quantity<=0")[0][0] == 0
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales WHERE order_status NOT IN "
                       "('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED')")[0][0] == 0
    # referential integrity of the fact
    assert wh_rows(wh, "SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_product p "
                       "ON p.product_id=f.product_id WHERE p.product_id IS NULL")[0][0] == 0
    # reconciliation: source items = fact + quarantined items
    src_items = info["items"]
    q_items = wh_rows(wh, "SELECT COUNT(*) FROM quarantine WHERE source='order_items'")[0][0]
    fact = wh_rows(wh, "SELECT COUNT(*) FROM fact_sales")[0][0]
    assert fact + q_items == src_items - (
        wh_rows(wh, "SELECT COUNT(*) FROM stg_items")[0][0] - fact)  # staged-but-unresolved are 0 here
    assert wh_rows(wh, "SELECT COUNT(*) FROM stg_items")[0][0] + q_items == src_items


def test_rerun_without_changes_is_idempotent(clean_src):
    (oltp, wh), _ = clean_src
    run_batch(oltp, wh)
    before = wh_rows(wh, "SELECT * FROM fact_sales ORDER BY item_id")
    run_batch(oltp, wh)
    assert wh_rows(wh, "SELECT * FROM fact_sales ORDER BY item_id") == before


def test_incremental_run_equals_full_rebuild(clean_src, tmp_path):
    (oltp, wh), _ = clean_src
    run_batch(oltp, wh)
    mutate(oltp, at="2031-01-01T00:00:00", new_orders=20, status_updates=15, price_changes=3)
    stats = run_batch(oltp, wh)
    assert stats["extracted"]["orders"] < 60  # incremental, not a full re-read
    fresh = str(tmp_path / "fresh.db")
    run_batch(oltp, fresh)
    q = "SELECT * FROM fact_sales ORDER BY item_id"
    assert wh_rows(wh, q) == wh_rows(fresh, q)


def test_status_update_flows_to_fact(clean_src):
    (oltp, wh), _ = clean_src
    run_batch(oltp, wh)
    src = sqlite3.connect(oltp)
    src.execute("UPDATE orders SET status='CANCELLED', updated_at='2031-02-02T00:00:00' WHERE order_id=1")
    src.commit()
    run_batch(oltp, wh)
    assert {r[0] for r in wh_rows(wh, "SELECT order_status FROM fact_sales WHERE order_id=1")} == {"CANCELLED"}


def test_failed_load_rolls_back_and_does_not_advance_watermark(clean_src, monkeypatch):
    (oltp, wh), _ = clean_src

    def boom(*a, **k):
        raise RuntimeError("simulated crash mid-load")
    monkeypatch.setattr(pipeline, "rebuild_fact", boom)
    with pytest.raises(RuntimeError):
        run_batch(oltp, wh)
    assert wh_rows(wh, "SELECT COUNT(*) FROM stg_orders")[0][0] == 0
    assert wh_rows(wh, "SELECT COUNT(*) FROM etl_state")[0][0] == 0
    monkeypatch.undo()
    stats = run_batch(oltp, wh)           # the retry loads everything
    assert stats["fact"]["fact_rows"] > 0


def test_quarantine_does_not_grow_on_reruns(tmp_path):
    oltp, wh = str(tmp_path / "o.db"), str(tmp_path / "w.db")
    generate(oltp, orders=300, dirty_rate=0.05, seed=2)
    run_batch(oltp, wh)
    n1 = wh_rows(wh, "SELECT COUNT(*) FROM quarantine")[0][0]
    run_batch(oltp, wh)
    assert wh_rows(wh, "SELECT COUNT(*) FROM quarantine")[0][0] == n1
