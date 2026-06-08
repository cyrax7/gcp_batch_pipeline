"""
monthly_performance — overall monthly KPIs with month-over-month growth trends.

Covers all orders (not just completed) to capture conversion and return rates.
Uses window lag() to compute MoM revenue and order volume growth.

Output table: <bq-project>.<bq-dataset>.monthly_performance (partitioned by ingestion_date)

Schema:
  year, month,
  total_orders, completed_orders, shipped_orders, returned_orders, cancelled_orders,
  unique_customers, total_revenue, avg_order_value, total_discounts,
  completion_rate_pct, return_rate_pct, cancellation_rate_pct,
  successful_transactions, transaction_success_rate_pct,
  revenue_mom_growth_pct, orders_mom_growth_pct,
  ingestion_date
"""
import logging
import sys

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lag,
    lit,
    month,
    round as spark_round,
    sum,
    to_date,
    when,
    year,
)
from pyspark.sql.types import DateType

from utils import (
    base_parser,
    build_spark,
    clean_customers,
    clean_orders,
    clean_transactions,
    read_csv,
    write_to_bq,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TABLE = "monthly_performance"


def transform(orders, customers, transactions, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    # enrich orders with customer and transaction info
    enriched = (
        orders
        .join(customers.select("customer_id", "region"), "customer_id", "left")
        .join(transactions.select("order_id", "transaction_status"), "order_id", "left")
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
    )

    # aggregate monthly KPIs
    monthly = (
        enriched
        .groupBy("year", "month")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(sum("discount_amount"), 2).alias("total_discounts"),
            count(when(col("order_status") == "completed", True)).alias("completed_orders"),
            count(when(col("order_status") == "shipped", True)).alias("shipped_orders"),
            count(when(col("order_status") == "returned", True)).alias("returned_orders"),
            count(when(col("order_status") == "cancelled", True)).alias("cancelled_orders"),
            count(when(col("transaction_status") == "success", True)).alias("successful_transactions"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_revenue") / col("total_orders"), 2),
        )
        .withColumn(
            "completion_rate_pct",
            spark_round(col("completed_orders") / col("total_orders") * 100, 2),
        )
        .withColumn(
            "return_rate_pct",
            spark_round(col("returned_orders") / col("total_orders") * 100, 2),
        )
        .withColumn(
            "cancellation_rate_pct",
            spark_round(col("cancelled_orders") / col("total_orders") * 100, 2),
        )
        .withColumn(
            "transaction_success_rate_pct",
            spark_round(col("successful_transactions") / col("total_orders") * 100, 2),
        )
    )

    # MoM growth — requires ordering by time, use window lag()
    time_window = Window.orderBy("year", "month")

    return (
        monthly
        .withColumn("prev_revenue", lag("total_revenue").over(time_window))
        .withColumn("prev_orders", lag("total_orders").over(time_window))
        .withColumn(
            "revenue_mom_growth_pct",
            when(
                col("prev_revenue").isNotNull() & (col("prev_revenue") > 0),
                spark_round(
                    (col("total_revenue") - col("prev_revenue")) / col("prev_revenue") * 100, 2
                ),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "orders_mom_growth_pct",
            when(
                col("prev_orders").isNotNull() & (col("prev_orders") > 0),
                spark_round(
                    (col("total_orders") - col("prev_orders")) / col("prev_orders") * 100, 2
                ),
            ).otherwise(lit(None)),
        )
        .drop("prev_revenue", "prev_orders")
        .withColumn("ingestion_date", ingestion_dt)
        .select(
            "year", "month",
            "total_orders", "completed_orders", "shipped_orders",
            "returned_orders", "cancelled_orders",
            "unique_customers", "total_revenue", "avg_order_value", "total_discounts",
            "completion_rate_pct", "return_rate_pct", "cancellation_rate_pct",
            "successful_transactions", "transaction_success_rate_pct",
            "revenue_mom_growth_pct", "orders_mom_growth_pct",
            "ingestion_date",
        )
        .orderBy("year", "month")
    )


def main():
    args = base_parser("Monthly performance KPIs with MoM growth").parse_args()
    spark = build_spark("MonthlyPerformance", args.bq_temp_bucket)

    base = f"gs://{args.gcs_bucket}/{args.gcs_prefix}"
    d = args.date
    log.info("Processing %s for date=%s", TABLE, d)

    customers = clean_customers(read_csv(spark, f"{base}/customers.csv"))
    orders = clean_orders(read_csv(spark, f"{base}/orders.csv"))
    transactions = clean_transactions(read_csv(spark, f"{base}/transactions.csv"))

    customers.cache()
    orders.cache()

    df = transform(orders, customers, transactions, d)
    table = write_to_bq(df, TABLE, args.bq_project, args.bq_dataset)
    log.info("Done — written to %s", table)
    spark.stop()


if __name__ == "__main__":
    main()
