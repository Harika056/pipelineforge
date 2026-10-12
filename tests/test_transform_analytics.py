import pandas as pd
import pytest

from pipelineforge import analytics
from pipelineforge.pipeline import run_batch
from pipelineforge.schema import WAREHOUSE_DDL, connect
from pipelineforge.transform import build_dim_date, build_fact, build_fact_pandas
from tests.conftest import wh_rows


def _joined():
    return pd.DataFrame({
        "item_id": [2, 1, 3], "order_id": [1, 1, 2], "customer_id": [5, 5, 6], "product_id": [9, 8, 9],
        "quantity": [2, 1, 3], "unit_price": [10.5, 3.25, 1.1],
        "status": ["PAID", "PAID", "SHIPPED"],
        "order_ts": ["2025-03-09T10:00:00", "2025-03-09T10:00:00", "2025-12-31T23:59:59"]})


def test_fact_columns_revenue_and_date_key():
    f = build_fact_pandas(_joined())
    assert list(f["item_id"]) == [1, 2, 3]                       # sorted
    assert list(f["date_key"]) == [20250309, 20250309, 20251231]
    assert list(f["revenue"]) == [3.25, 21.0, 3.3]
    assert list(f["order_status"]) == ["PAID", "PAID", "SHIPPED"]


def test_empty_input_gives_empty_fact_with_columns():
    f = build_fact_pandas(pd.DataFrame())
    assert f.empty and "revenue" in f.columns


def test_unknown_engine_rejected():
    with pytest.raises(ValueError):
        build_fact(_joined(), engine="duckdb")


def test_dim_date_from_fact():
    d = build_dim_date(build_fact_pandas(_joined()))
    assert sorted(d["date_key"]) == [20250309, 20251231]
    row = d[d["date_key"] == 20250309].iloc[0]
    assert (row["year"], row["month"], row["day"], row["weekday"]) == (2025, 3, 9, 6)   # a Sunday


def _spark_or_skip():
    pytest.importorskip("pyspark")
    from pipelineforge.spark_transform import get_spark
    try:
        get_spark()
    except Exception as e:                       # no Java, or Spark cannot start on this machine
        pytest.skip(f"Spark could not start: {type(e).__name__}")


def test_spark_engine_matches_pandas_engine():
    _spark_or_skip()
    pd_fact = build_fact(_joined(), "pandas")
    sp_fact = build_fact(_joined(), "spark")
    pd.testing.assert_frame_equal(pd_fact.reset_index(drop=True), sp_fact.reset_index(drop=True),
                                  check_dtype=False)


def test_spark_full_pipeline_equals_pandas_pipeline(clean_src, tmp_path):
    _spark_or_skip()
    (oltp, wh_pandas), _ = clean_src
    wh_spark = str(tmp_path / "spark.db")
    run_batch(oltp, wh_pandas, "pandas")
    run_batch(oltp, wh_spark, "spark")
    q = "SELECT * FROM fact_sales ORDER BY item_id"
    assert wh_rows(wh_pandas, q) == wh_rows(wh_spark, q)


def test_analytics_queries_run_and_exclude_cancelled(clean_src):
    (oltp, wh), _ = clean_src
    run_batch(oltp, wh)
    conn = connect(wh, WAREHOUSE_DDL)
    top = analytics.run(conn, "top_customers")
    assert 0 < len(top) <= 10 and top["revenue"].is_monotonic_decreasing
    monthly = analytics.run(conn, "revenue_by_category_month")
    non_cancelled = wh_rows(wh, "SELECT ROUND(SUM(revenue),2) FROM fact_sales WHERE order_status<>'CANCELLED'")[0][0]
    assert monthly["revenue"].sum() == pytest.approx(non_cancelled, abs=0.05)
