"""Shared utilities: SparkSession, readers, writers, and cleaners."""
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lower, row_number, to_date, trim
from pyspark.sql.types import DateType, DoubleType, IntegerType


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--date", default=datetime.now().strftime("%Y%m%d"),
                   help="Date suffix of input files (YYYYMMDD)")
    p.add_argument("--gcs-bucket", required=True)
    p.add_argument("--gcs-prefix", required=True,
                   help="GCS path prefix under the bucket where source CSVs live")
    p.add_argument("--bq-project", required=True)
    p.add_argument("--bq-dataset", required=True)
    p.add_argument("--bq-temp-bucket", required=True)
    return p


def build_spark(app_name: str, bq_temp_bucket: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("temporaryGcsBucket", bq_temp_bucket)
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def write_to_bq(df, table_name: str, bq_project: str, bq_dataset: str) -> str:
    full_table = f"{bq_project}.{bq_dataset}.{table_name}"
    (
        df.write
        .format("bigquery")
        .option("table", full_table)
        .option("partitionField", "ingestion_date")
        .option("partitionType", "DAY")
        .mode("append")
        .save()
    )
    return full_table


# ---------------------------------------------------------------------------
# Cleaners
# ---------------------------------------------------------------------------

def clean_customers(df):
    return (
        df
        .dropna(subset=["customer_id", "email"])
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("region", trim(col("region")))
        .withColumn("customer_segment", trim(col("customer_segment")))
        .dropDuplicates(["customer_id"])
    )


def clean_products(df):
    return (
        df
        .dropna(subset=["product_id"])
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("sub_category", trim(col("sub_category")))
        .withColumn("brand", trim(col("brand")))
        .dropDuplicates(["product_id"])
    )


def clean_orders(df):
    valid_statuses = ["completed", "shipped", "returned", "cancelled"]
    return (
        df
        .dropna(subset=["order_id", "customer_id"])
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("discount_amount", col("discount_amount").cast(DoubleType()))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .filter(col("order_status").isin(valid_statuses))
        .dropDuplicates(["order_id"])
    )


def clean_order_items(df):
    return (
        df
        .dropna(subset=["order_item_id", "order_id", "product_id"])
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("price_per_unit", col("price_per_unit").cast(DoubleType()))
        .filter((col("quantity") > 0) & (col("price_per_unit") > 0))
        .dropDuplicates(["order_item_id"])
    )


def clean_transactions(df):
    # one transaction per order — keep the latest when multiple exist
    return (
        df
        .dropna(subset=["transaction_id", "order_id"])
        .withColumn("transaction_date", to_date(col("transaction_date")))
        .withColumn("transaction_amount", col("transaction_amount").cast(DoubleType()))
        .withColumn("transaction_status", lower(trim(col("transaction_status"))))
        .withColumn(
            "_rank",
            row_number().over(
                Window.partitionBy("order_id").orderBy(col("transaction_date").desc())
            ),
        )
        .filter(col("_rank") == 1)
        .drop("_rank")
    )
