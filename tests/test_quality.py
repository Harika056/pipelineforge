import pandas as pd

from pipelineforge.quality import validate_customers, validate_items, validate_orders, validate_products


def test_customer_rules():
    df = pd.DataFrame({"customer_id": [1, 2, 3], "name": list("abc"),
                       "email": ["a@x.com", None, "nope"], "country": ["IN", "US", None],
                       "updated_at": ["t"] * 3})
    clean, rejects, results = validate_customers(df)
    assert list(clean["customer_id"]) == [1]
    assert {r["row_key"]: r["reason"] for r in rejects} == {"2": "email_valid", "3": "email_valid"}
    assert sum(r.failed for r in results) == 2


def test_each_row_is_attributed_to_first_failing_rule_only():
    df = pd.DataFrame({"item_id": [1], "order_id": [99], "product_id": [99], "quantity": [-1],
                       "unit_price": [1.0], "updated_at": ["t"]})
    clean, rejects, results = validate_items(df, known_orders=set(), known_products=set())
    assert clean.empty and len(rejects) == 1 and rejects[0]["reason"] == "quantity_positive"


def test_order_rules():
    df = pd.DataFrame({"order_id": [1, 2, 3, 4], "customer_id": [1, 1, 1, 9],
                       "status": ["PAID", "WAT", "PAID", "PAID"],
                       "order_ts": ["2025-01-01T10:00:00", "2025-01-01T10:00:00", "garbage", "2025-01-01T10:00:00"],
                       "updated_at": ["t"] * 4})
    clean, rejects, _ = validate_orders(df, known_customers={1})
    assert list(clean["order_id"]) == [1]
    reasons = {r["row_key"]: r["reason"] for r in rejects}
    assert reasons == {"2": "status_valid", "3": "order_ts_valid", "4": "customer_exists"}


def test_product_negative_price_rejected():
    df = pd.DataFrame({"product_id": [1, 2], "name": ["a", "b"], "category": ["x", "x"],
                       "unit_price": [5.0, -1.0], "updated_at": ["t", "t"]})
    clean, rejects, _ = validate_products(df)
    assert list(clean["product_id"]) == [1] and rejects[0]["reason"] == "price_non_negative"


def test_empty_input_ok():
    clean, rejects, results = validate_items(
        pd.DataFrame(columns=["item_id", "order_id", "product_id", "quantity", "unit_price", "updated_at"]),
        set(), set())
    assert clean.empty and rejects == []
