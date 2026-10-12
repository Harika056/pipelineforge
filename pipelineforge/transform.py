"""Pure transforms. `build_fact` has two interchangeable engines (pandas default, PySpark optional)
that must produce identical output; tests compare them."""
from __future__ import annotations

import pandas as pd

FACT_COLS = ["item_id", "order_id", "customer_id", "product_id", "date_key", "quantity",
             "unit_price", "revenue", "order_status"]


def build_fact_pandas(joined: pd.DataFrame) -> pd.DataFrame:
    """joined: one row per order item with order fields (customer_id, status, order_ts) attached."""
    if joined.empty:
        return pd.DataFrame(columns=FACT_COLS)
    d = joined.copy()
    ts = pd.to_datetime(d["order_ts"], format="%Y-%m-%dT%H:%M:%S")
    d["date_key"] = (ts.dt.year * 10000 + ts.dt.month * 100 + ts.dt.day).astype("int64")
    d["revenue"] = (d["quantity"] * d["unit_price"]).round(2)
    d["order_status"] = d["status"]
    return d[FACT_COLS].sort_values("item_id").reset_index(drop=True)


def build_fact(joined: pd.DataFrame, engine: str = "pandas") -> pd.DataFrame:
    if engine == "spark":
        from .spark_transform import build_fact_spark
        return build_fact_spark(joined)
    if engine != "pandas":
        raise ValueError(f"unknown engine {engine!r}")
    return build_fact_pandas(joined)


def build_dim_date(fact: pd.DataFrame) -> pd.DataFrame:
    if fact.empty:
        return pd.DataFrame(columns=["date_key", "date", "year", "month", "day", "weekday"])
    keys = fact["date_key"].drop_duplicates().astype("int64")
    dates = pd.to_datetime(keys.astype(str), format="%Y%m%d")
    return pd.DataFrame({"date_key": keys.values, "date": dates.dt.strftime("%Y-%m-%d").values,
                         "year": dates.dt.year.values, "month": dates.dt.month.values,
                         "day": dates.dt.day.values, "weekday": dates.dt.weekday.values})
