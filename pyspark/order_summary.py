"""
order_summary — enriched per-order view joining all 5 source tables.

Output table: <bq-project>.<bq-dataset>.order_summary (partitioned by ingestion_date)

Schema:
  order_id, customer_id, customer_name, email, region, customer_segment,
  signup_date, order_date, order_status, payment_method,
  total_amount, discount_amount, discount_pct, net_amount,
  total_line_items, total_quantity, items_revenue, top_category,
  transaction_id, transaction_status, transaction_date, transaction_amount,
  ingestion_date
"""
import logging
import sys

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    count,
    lit,
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

TABLE = "order_summary"


def transform(orders, customers, order_items, products, transactions, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    # top category per order via window (most items from that category)
    cat_window = Window.partitionBy("order_id").orderBy(col("cat_count").desc())
    top_category_per_order = (
        order_items
        .join(products.select("product_id", "category"), "product_id", "left")
        .groupBy("order_id", "category")
        .agg(count("order_item_id").alias("cat_count"))
        .withColumn("cat_rank", row_number().over(cat_window))
        .filter(col("cat_rank") == 1)
        .select("order_id", col("category").alias("top_category"))
    )

    # item-level aggregates per order
    items_agg = (
        order_items
        .groupBy("order_id")
        .agg(
            count("order_item_id").alias("total_line_items"),
            sum("quantity").alias("total_quantity"),
            spark_round(sum(col("quantity") * col("price_per_unit")), 2).alias("items_revenue"),
        )
    )

    return (
        orders
        .join(
            customers.select(
                "customer_id", "customer_name", "email",
                "region", "customer_segment", "signup_date",
            ),
            "customer_id", "left",
        )
        .join(items_agg, "order_id", "left")
        .join(top_category_per_order, "order_id", "left")
        .join(
            transactions.select(
                "order_id", "transaction_id",
                "transaction_status", "transaction_date", "transaction_amount",
            ),
            "order_id", "left",
        )
        .withColumn("net_amount", spark_round(col("total_amount") - col("discount_amount"), 2))
        .withColumn(
            "discount_pct",
            when(col("total_amount") > 0,
                 spark_round(col("discount_amount") / col("total_amount") * 100, 2))
            .otherwise(lit(0.0)),
        )
        .withColumn("ingestion_date", ingestion_dt)
        .select(
            "order_id", "customer_id", "customer_name", "email",
            "region", "customer_segment", "signup_date",
            "order_date", "order_status", "payment_method",
            "total_amount", "discount_amount", "discount_pct", "net_amount",
            "total_line_items", "total_quantity", "items_revenue", "top_category",
            "transaction_id", "transaction_status", "transaction_date", "transaction_amount",
            "ingestion_date",
        )
    )


def main():
    args = base_parser("Order summary transformation").parse_args()
    spark = build_spark("OrderSummary", args.bq_temp_bucket)

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
