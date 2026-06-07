"""
customer_retention — cohort-based retention analysis.

Groups customers by their signup month (cohort) and tracks how many
from each cohort placed orders in each subsequent month. Produces
a retention matrix useful for churn analysis in Looker Studio.

Output table: YOUR_BQ_DATASET.customer_retention (partitioned by ingestion_date)

Schema:
  cohort_year, cohort_month, activity_year, activity_month,
  periods_since_cohort (months elapsed since signup month),
  cohort_size, active_customers, retention_rate_pct,
  ingestion_date
"""
import logging
import sys

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    month,
    round as spark_round,
    to_date,
    year,
)
from pyspark.sql.types import DateType

from utils import (
    base_parser,
    build_spark,
    clean_customers,
    clean_orders,
    read_csv,
    write_to_bq,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TABLE = "customer_retention"


def transform(orders, customers, date: str):
    ingestion_dt = to_date(lit(date), "yyyyMMdd").cast(DateType())

    # assign each customer to a signup cohort (year + month)
    customers_with_cohort = (
        customers
        .withColumn("cohort_year", year(col("signup_date")))
        .withColumn("cohort_month", month(col("signup_date")))
    )

    # cohort size: how many customers signed up per cohort month
    cohort_sizes = (
        customers_with_cohort
        .groupBy("cohort_year", "cohort_month")
        .agg(count("customer_id").alias("cohort_size"))
    )

    # activity: which months did each customer place an order
    orders_with_activity = (
        orders
        .withColumn("activity_year", year(col("order_date")))
        .withColumn("activity_month", month(col("order_date")))
    )

    # join orders → cohort info, then compute months elapsed since cohort
    joined = (
        orders_with_activity
        .join(
            customers_with_cohort.select(
                "customer_id", "cohort_year", "cohort_month"
            ),
            "customer_id", "inner",
        )
        .withColumn(
            "periods_since_cohort",
            (col("activity_year") - col("cohort_year")) * 12
            + (col("activity_month") - col("cohort_month")),
        )
        .filter(col("periods_since_cohort") >= 0)
    )

    # active customers per cohort + activity period
    activity = (
        joined
        .groupBy(
            "cohort_year", "cohort_month",
            "activity_year", "activity_month",
            "periods_since_cohort",
        )
        .agg(countDistinct("customer_id").alias("active_customers"))
    )

    # attach cohort size and compute retention rate
    return (
        activity
        .join(cohort_sizes, ["cohort_year", "cohort_month"], "left")
        .withColumn(
            "retention_rate_pct",
            spark_round(col("active_customers") / col("cohort_size") * 100, 2),
        )
        .withColumn("ingestion_date", ingestion_dt)
        .select(
            "cohort_year", "cohort_month",
            "activity_year", "activity_month",
            "periods_since_cohort",
            "cohort_size", "active_customers", "retention_rate_pct",
            "ingestion_date",
        )
        .orderBy("cohort_year", "cohort_month", "periods_since_cohort")
    )


def main():
    args = base_parser("Cohort-based customer retention analysis").parse_args()
    spark = build_spark("CustomerRetention", args.bq_temp_bucket)

    base = f"gs://{args.gcs_bucket}/{args.gcs_prefix}"
    d = args.date
    log.info("Processing %s for date=%s", TABLE, d)

    customers = clean_customers(read_csv(spark, f"{base}/customers.csv"))
    orders = clean_orders(read_csv(spark, f"{base}/orders.csv"))

    customers.cache()
    orders.cache()

    df = transform(orders, customers, d)
    table = write_to_bq(df, TABLE, args.bq_project, args.bq_dataset)
    log.info("Done — written to %s", table)
    spark.stop()


if __name__ == "__main__":
    main()
