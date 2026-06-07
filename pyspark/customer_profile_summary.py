"""
customer_profile_summary — per-customer behavioral profile.

Uses window functions to derive:
  - favorite_category  (most-ordered product category)
  - last_order_status  (most recent order's status)
  - last_payment_method

Output table: YOUR_BQ_DATASET.customer_profile_summary (partitioned by ingestion_date)

Schema:
  customer_id, customer_name, email, region, customer_segment, signup_date,
  total_orders, completed_orders, total_spent, avg_order_value,
  total_discounts_received, first_purchase_date, last_purchase_date,
  days_since_last_purchase, customer_tenure_days,
  favorite_category, last_order_status, last_payment_method,
  ingestion_date
"""
import logging
import sys

from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    datediff,
    lit,
    max,
    min,
    round as spark_round,
    row_number,
    sum,
    to_date,
    when,
)
from pyspark.sql.types import DateType

from utils import (
    base_parser,
    build_spark,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
    clean_transactions,
    read_csv,
    write_to_bq,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TABLE = "customer_profile_summary"


def transform(orders, customers, order_items, products, transactions, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    # core order-level metrics per customer
    cust_metrics = (
        orders
        .join(customers.select(
            "customer_id", "customer_name", "email",
            "region", "customer_segment", "signup_date",
        ), "customer_id", "left")
        .join(transactions.select("order_id", "transaction_status"), "order_id", "left")
        .groupBy("customer_id", "customer_name", "email",
                 "region", "customer_segment", "signup_date")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct(
                when(col("order_status") == "completed", col("order_id"))
            ).alias("completed_orders"),
            spark_round(sum("total_amount"), 2).alias("total_spent"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_round(sum("discount_amount"), 2).alias("total_discounts_received"),
            min("order_date").alias("first_purchase_date"),
            max("order_date").alias("last_purchase_date"),
        )
        .withColumn(
            "days_since_last_purchase",
            datediff(ingestion_dt, col("last_purchase_date")),
        )
        .withColumn(
            "customer_tenure_days",
            datediff(col("last_purchase_date"), col("signup_date")),
        )
    )

    # window: favorite category (most order-items purchased from that category)
    cat_window = Window.partitionBy("customer_id").orderBy(col("item_count").desc())
    fav_category = (
        order_items
        .join(products.select("product_id", "category"), "product_id", "left")
        .join(orders.select("order_id", "customer_id"), "order_id", "left")
        .groupBy("customer_id", "category")
        .agg(count("order_item_id").alias("item_count"))
        .withColumn("cat_rank", row_number().over(cat_window))
        .filter(col("cat_rank") == 1)
        .select("customer_id", col("category").alias("favorite_category"))
    )

    # window: most recent order → last_order_status, last_payment_method
    recency_window = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
    last_order = (
        orders
        .withColumn("order_rank", row_number().over(recency_window))
        .filter(col("order_rank") == 1)
        .select(
            "customer_id",
            col("order_status").alias("last_order_status"),
            col("payment_method").alias("last_payment_method"),
        )
    )

    return (
        cust_metrics
        .join(fav_category, "customer_id", "left")
        .join(last_order, "customer_id", "left")
        .withColumn("ingestion_date", ingestion_dt)
        .select(
            "customer_id", "customer_name", "email", "region", "customer_segment", "signup_date",
            "total_orders", "completed_orders", "total_spent", "avg_order_value",
            "total_discounts_received", "first_purchase_date", "last_purchase_date",
            "days_since_last_purchase", "customer_tenure_days",
            "favorite_category", "last_order_status", "last_payment_method",
            "ingestion_date",
        )
    )


def main():
    args = base_parser("Per-customer behavioral profile").parse_args()
    spark = build_spark("CustomerProfileSummary", args.bq_temp_bucket)

    base = f"gs://{args.gcs_bucket}/{args.gcs_prefix}"
    d = args.date
    log.info("Processing %s for date=%s", TABLE, d)

    customers = clean_customers(read_csv(spark, f"{base}/customers.csv"))
    products = clean_products(read_csv(spark, f"{base}/products.csv"))
    orders = clean_orders(read_csv(spark, f"{base}/orders.csv"))
    order_items = clean_order_items(read_csv(spark, f"{base}/order_items.csv"))
    transactions = clean_transactions(read_csv(spark, f"{base}/transactions.csv"))

    customers.cache()
    products.cache()
    orders.cache()

    df = transform(orders, customers, order_items, products, transactions, d)
    table = write_to_bq(df, TABLE, args.bq_project, args.bq_dataset)
    log.info("Done — written to %s", table)
    spark.stop()


if __name__ == "__main__":
    main()
