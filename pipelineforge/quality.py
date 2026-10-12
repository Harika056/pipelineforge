"""Data-quality rules. Each row is attributed to the FIRST rule it fails; failing rows are
quarantined (kept with a reason) instead of loaded."""
from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd

from .schema import VALID_STATUS


@dataclass
class RuleResult:
    source: str
    rule: str
    failed: int
    total: int


def _apply(source: str, df: pd.DataFrame, key: str, rules):
    """rules: list of (name, fn(df) -> boolean Series that is True where the row is VALID)."""
    total = len(df)
    results, rejects = [], []
    remaining = df
    for name, fn in rules:
        if remaining.empty:
            results.append(RuleResult(source, name, 0, total))
            continue
        ok = fn(remaining).fillna(False).astype(bool)
        bad = remaining[~ok]
        for _, row in bad.iterrows():
            rejects.append({"source": source, "row_key": str(row[key]), "reason": name,
                            "payload": json.dumps({k: (None if pd.isna(v) else v) for k, v in row.items()},
                                                  default=str)})
        results.append(RuleResult(source, name, len(bad), total))
        remaining = remaining[ok]
    return remaining.copy(), rejects, results


def _is_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%S", errors="coerce").notna()


def validate_customers(df: pd.DataFrame):
    return _apply("customers", df, "customer_id", [
        ("customer_id_not_null", lambda d: d["customer_id"].notna()),
        ("customer_id_unique", lambda d: ~d["customer_id"].duplicated(keep="last")),
        ("email_valid", lambda d: d["email"].astype("string").str.contains("@", na=False)),
        ("country_not_null", lambda d: d["country"].notna()),
    ])


def validate_products(df: pd.DataFrame):
    return _apply("products", df, "product_id", [
        ("product_id_unique", lambda d: ~d["product_id"].duplicated(keep="last")),
        ("name_not_null", lambda d: d["name"].notna()),
        ("price_non_negative", lambda d: pd.to_numeric(d["unit_price"], errors="coerce") >= 0),
    ])


def validate_orders(df: pd.DataFrame, known_customers: set):
    return _apply("orders", df, "order_id", [
        ("order_id_unique", lambda d: ~d["order_id"].duplicated(keep="last")),
        ("status_valid", lambda d: d["status"].isin(VALID_STATUS)),
        ("order_ts_valid", lambda d: _is_ts(d["order_ts"])),
        ("customer_exists", lambda d: d["customer_id"].isin(known_customers)),
    ])


def validate_items(df: pd.DataFrame, known_orders: set, known_products: set):
    return _apply("order_items", df, "item_id", [
        ("item_id_unique", lambda d: ~d["item_id"].duplicated(keep="last")),
        ("quantity_positive", lambda d: pd.to_numeric(d["quantity"], errors="coerce") > 0),
        ("price_non_negative", lambda d: pd.to_numeric(d["unit_price"], errors="coerce") >= 0),
        ("order_exists", lambda d: d["order_id"].isin(known_orders)),
        ("product_exists", lambda d: d["product_id"].isin(known_products)),
    ])
