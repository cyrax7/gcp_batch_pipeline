"""
monthly_sales — revenue aggregated by year / month / region / product category.

Only counts completed orders with successful transactions.

Output table: <bq-project>.<bq-dataset>.monthly_sales (partitioned by ingestion_date)

Schema:
  year, month, region, category,
  total_orders, unique_customers, gross_revenue,
  total_units_sold, avg_order_value, ingestion_date
"""
import logging
import sys

from pyspark.sql.functions import (
    col,
    countDistinct,
    lit,
    month,
    round as spark_round,
    sum,
    to_date,
    year,
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

TABLE = "monthly_sales"


def transform(orders, customers, order_items, products, transactions, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    flat = (
        order_items
        .join(products.select("product_id", "category", "sub_category"), "product_id", "left")
        .join(
            orders.select("order_id", "customer_id", "order_date", "order_status"),
            "order_id", "left",
        )
        .join(customers.select("customer_id", "region"), "customer_id", "left")
        .join(transactions.select("order_id", "transaction_status"), "order_id", "left")
        .filter(
            (col("order_status") == "completed") & (col("transaction_status") == "success")
        )
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("line_revenue", spark_round(col("quantity") * col("price_per_unit"), 2))
    )

    return (
        flat
        .groupBy("year", "month", "region", "category")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(sum("line_revenue"), 2).alias("gross_revenue"),
            sum("quantity").alias("total_units_sold"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("gross_revenue") / col("total_orders"), 2),
        )
        .withColumn("ingestion_date", ingestion_dt)
        .orderBy("year", "month", "region", "category")
    )


def main():
    args = base_parser("Monthly sales by region and category").parse_args()
    spark = build_spark("MonthlySales", args.bq_temp_bucket)

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
