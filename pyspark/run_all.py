"""
run_all — executes all 6 transformations in a single Spark session.

Source CSVs are read and cleaned once, then shared across every transform.
Submit this as a single Dataproc job instead of running each file separately.

Usage:
  gcloud dataproc jobs submit pyspark gs://BUCKET/pyspark/run_all.py \
    --cluster=CLUSTER --region=REGION \
    --py-files=gs://BUCKET/pyspark/utils.py,gs://BUCKET/pyspark/order_summary.py,\
gs://BUCKET/pyspark/monthly_sales.py,gs://BUCKET/pyspark/monthly_performance.py,\
gs://BUCKET/pyspark/customer_profile_summary.py,\
gs://BUCKET/pyspark/customer_transactions.py,\
gs://BUCKET/pyspark/customer_retention.py \
    -- \
    --date=YYYYMMDD \
    --gcs-bucket=BUCKET \
    --bq-project=PROJECT \
    --bq-dataset=DATASET \
    --bq-temp-bucket=BUCKET
"""
import logging
import sys

import customer_profile_summary
import customer_retention
import customer_transactions
import monthly_performance
import monthly_sales
import order_summary
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

JOBS = [
    ("order_summary",            order_summary.TABLE,            order_summary.transform),
    ("monthly_sales",            monthly_sales.TABLE,            monthly_sales.transform),
    ("monthly_performance",      monthly_performance.TABLE,      monthly_performance.transform),
    ("customer_profile_summary", customer_profile_summary.TABLE, customer_profile_summary.transform),
    ("customer_transactions",    customer_transactions.TABLE,    customer_transactions.transform),
    ("customer_retention",       customer_retention.TABLE,       customer_retention.transform),
]


def run_transform(name, table, transform_fn, context):
    log.info("━━━ Starting: %s ━━━", name)
    try:
        df = transform_fn(**context[name])
        written = write_to_bq(df, table, context["bq_project"], context["bq_dataset"])
        log.info("✓ Done: %s → %s", name, written)
        return True
    except Exception:
        log.exception("✗ Failed: %s", name)
        return False


def main():
    args = base_parser("Run all 6 sales transformations in one Spark session").parse_args()
    spark = build_spark("SalesTransformationAll", args.bq_temp_bucket)

    base = f"gs://{args.gcs_bucket}/{args.gcs_prefix}"
    d = args.date

    log.info("Reading and cleaning source tables for date=%s ...", d)

    customers   = clean_customers(read_csv(spark, f"{base}/customers.csv"))
    products    = clean_products(read_csv(spark, f"{base}/products.csv"))
    orders      = clean_orders(read_csv(spark, f"{base}/orders.csv"))
    order_items = clean_order_items(read_csv(spark, f"{base}/order_items.csv"))
    transactions = clean_transactions(read_csv(spark, f"{base}/transactions.csv"))

    # cache tables that are reused across multiple transforms
    customers.cache()
    products.cache()
    orders.cache()
    order_items.cache()
    transactions.cache()

    log.info("All tables cleaned and cached. Running transformations...")

    # each transform receives only the DataFrames it needs
    context = {
        "bq_project": args.bq_project,
        "bq_dataset": args.bq_dataset,
        "order_summary": {
            "orders": orders, "customers": customers,
            "order_items": order_items, "products": products,
            "transactions": transactions, "date": d,
        },
        "monthly_sales": {
            "orders": orders, "customers": customers,
            "order_items": order_items, "products": products,
            "transactions": transactions, "date": d,
        },
        "monthly_performance": {
            "orders": orders, "customers": customers,
            "transactions": transactions, "date": d,
        },
        "customer_profile_summary": {
            "orders": orders, "customers": customers,
            "order_items": order_items, "products": products,
            "transactions": transactions, "date": d,
        },
        "customer_transactions": {
            "orders": orders, "customers": customers,
            "transactions": transactions, "date": d,
        },
        "customer_retention": {
            "orders": orders, "customers": customers, "date": d,
        },
    }

    results = {}
    for name, table, transform_fn in JOBS:
        results[name] = run_transform(name, table, transform_fn, context)

    # summary report
    log.info("━━━ Pipeline summary ━━━")
    passed = [n for n, ok in results.items() if ok]
    failed = [n for n, ok in results.items() if not ok]

    for name in passed:
        log.info("  ✓ %s", name)
    for name in failed:
        log.error("  ✗ %s", name)

    log.info("%d/%d transformations succeeded for date=%s", len(passed), len(JOBS), d)

    spark.stop()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
