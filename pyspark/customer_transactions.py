"""
customer_transactions — financial transaction summary per customer.

Uses a window function to derive preferred_payment_method
(the payment method used most frequently by the customer).

Output table: <bq-project>.<bq-dataset>.customer_transactions (partitioned by ingestion_date)

Schema:
  customer_id, customer_name, email, region, customer_segment,
  total_transactions, successful_transactions, failed_transactions,
  transaction_success_rate_pct,
  total_transaction_value, avg_transaction_value,
  max_transaction_value, min_transaction_value,
  first_transaction_date, last_transaction_date,
  preferred_payment_method,
  ingestion_date
"""
import logging
import sys

from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
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
    clean_orders,
    clean_transactions,
    read_csv,
    write_to_bq,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TABLE = "customer_transactions"


def transform(orders, customers, transactions, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    # join transactions → orders → customers for full context
    enriched = (
        transactions
        .join(orders.select("order_id", "customer_id", "payment_method"), "order_id", "left")
        .join(
            customers.select("customer_id", "customer_name", "email",
                             "region", "customer_segment"),
            "customer_id", "left",
        )
    )

    # customer-level transaction aggregates
    txn_metrics = (
        enriched
        .groupBy("customer_id", "customer_name", "email", "region", "customer_segment")
        .agg(
            count("transaction_id").alias("total_transactions"),
            count(when(col("transaction_status") == "success", True)).alias("successful_transactions"),
            count(when(col("transaction_status") == "failed", True)).alias("failed_transactions"),
            spark_round(sum("transaction_amount"), 2).alias("total_transaction_value"),
            spark_round(avg("transaction_amount"), 2).alias("avg_transaction_value"),
            spark_round(max("transaction_amount"), 2).alias("max_transaction_value"),
            spark_round(min("transaction_amount"), 2).alias("min_transaction_value"),
            min("transaction_date").alias("first_transaction_date"),
            max("transaction_date").alias("last_transaction_date"),
        )
        .withColumn(
            "transaction_success_rate_pct",
            spark_round(col("successful_transactions") / col("total_transactions") * 100, 2),
        )
    )

    # window: preferred payment method (most used per customer)
    pay_window = Window.partitionBy("customer_id").orderBy(col("pay_count").desc())
    preferred_payment = (
        enriched
        .groupBy("customer_id", "payment_method")
        .agg(count("transaction_id").alias("pay_count"))
        .withColumn("pay_rank", row_number().over(pay_window))
        .filter(col("pay_rank") == 1)
        .select("customer_id", col("payment_method").alias("preferred_payment_method"))
    )

    return (
        txn_metrics
        .join(preferred_payment, "customer_id", "left")
        .withColumn("ingestion_date", ingestion_dt)
        .select(
            "customer_id", "customer_name", "email", "region", "customer_segment",
            "total_transactions", "successful_transactions", "failed_transactions",
            "transaction_success_rate_pct",
            "total_transaction_value", "avg_transaction_value",
            "max_transaction_value", "min_transaction_value",
            "first_transaction_date", "last_transaction_date",
            "preferred_payment_method",
            "ingestion_date",
        )
    )


def main():
    args = base_parser("Customer-level transaction summary").parse_args()
    spark = build_spark("CustomerTransactions", args.bq_temp_bucket)

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
