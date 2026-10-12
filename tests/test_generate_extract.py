import sqlite3

from pipelineforge.extract import extract, get_watermarks
from pipelineforge.generate import generate, mutate
from pipelineforge.schema import OLTP_DDL, WAREHOUSE_DDL, connect


def test_generate_is_deterministic_and_plants_known_bad_rows(tmp_path):
    a = generate(str(tmp_path / "a.db"), orders=300, dirty_rate=0.05, seed=1)
    b = generate(str(tmp_path / "b.db"), orders=300, dirty_rate=0.05, seed=1)
    assert a == b
    assert sum(a["planted"].values()) > 0
    clean = generate(str(tmp_path / "c.db"), orders=300, dirty_rate=0.0, seed=1)
    assert sum(clean["planted"].values()) == 0


def test_first_extract_reads_everything(clean_src):
    (oltp_p, wh_p), info = clean_src
    oltp, wh = connect(oltp_p, OLTP_DDL), connect(wh_p, WAREHOUSE_DDL)
    data = extract(oltp, get_watermarks(wh))
    assert len(data["orders"]) == info["orders"]
    assert len(data["order_items"]) == info["items"]
    assert data["watermarks"]["orders"] != ""


def test_incremental_extract_only_returns_changed_orders_with_all_their_items(clean_src):
    (oltp_p, wh_p), info = clean_src
    oltp, wh = connect(oltp_p, OLTP_DDL), connect(wh_p, WAREHOUSE_DDL)
    wm = extract(oltp, get_watermarks(wh))["watermarks"]
    # touch ONE item of ONE order: the whole order (all items) must come back
    oltp.execute("UPDATE order_items SET quantity=quantity+1, updated_at='2030-01-01T00:00:00' WHERE item_id=1")
    order_id = oltp.execute("SELECT order_id FROM order_items WHERE item_id=1").fetchone()[0]
    n_items = oltp.execute("SELECT COUNT(*) FROM order_items WHERE order_id=?", (order_id,)).fetchone()[0]
    data = extract(oltp, wm)
    got = list(data["orders"]["order_id"])
    # the order whose item changed is returned; the newest row at the old watermark is also re-read
    # on purpose (>= watermark), so at most one extra order may appear
    assert order_id in got and len(got) <= 2
    returned_items = data["order_items"][data["order_items"]["order_id"] == order_id]
    assert len(returned_items) == n_items          # ALL items of the changed order come back
    assert data["watermarks"]["order_items"] == "2030-01-01T00:00:00"


def test_mutate_changes_are_picked_up(clean_src):
    (oltp_p, wh_p), _ = clean_src
    oltp, wh = connect(oltp_p, OLTP_DDL), connect(wh_p, WAREHOUSE_DDL)
    wm = extract(oltp, get_watermarks(wh))["watermarks"]
    mutate(oltp_p, at="2031-01-01T00:00:00", new_orders=5, status_updates=3, price_changes=2)
    data = extract(oltp, wm)
    got = set(data["orders"]["order_id"])
    assert {101, 102, 103, 104, 105} <= got            # every new order is picked up
    assert len(got) < 20                               # incremental: nowhere near all 105 orders
    changed = data["orders"][data["orders"]["updated_at"] >= "2031-01-01"]
    assert len(changed) >= 5 + 1                       # 5 new orders + at least one status update
