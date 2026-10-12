"""PySpark version of the fact transform (optional: needs `pip install pyspark` and Java 17).

On Windows, Spark may print winutils/Hadoop warnings; these do not affect this local, in-memory use.
"""
from __future__ import annotations

import os
import sys

import pandas as pd

from .transform import FACT_COLS

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder.master("local[2]").appName("pipelineforge")
                  .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
                  .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def build_fact_spark(joined: pd.DataFrame) -> pd.DataFrame:
    if joined.empty:
        return pd.DataFrame(columns=FACT_COLS)
    from pyspark.sql import functions as F
    spark = get_spark()
    sdf = spark.createDataFrame(joined[["item_id", "order_id", "customer_id", "product_id", "quantity",
                                        "unit_price", "status", "order_ts"]].astype(
        {"item_id": "int64", "order_id": "int64", "customer_id": "int64", "product_id": "int64",
         "quantity": "int64", "unit_price": "float64"}))
    ts = F.to_timestamp("order_ts", "yyyy-MM-dd'T'HH:mm:ss")
    out = (sdf.withColumn("date_key", (F.year(ts) * 10000 + F.month(ts) * 100 + F.dayofmonth(ts)).cast("long"))
              .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
              .withColumn("order_status", F.col("status"))
              .select(*FACT_COLS).orderBy("item_id"))
    return out.toPandas().reset_index(drop=True)
