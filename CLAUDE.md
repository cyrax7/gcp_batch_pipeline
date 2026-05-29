# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

GCP batch data pipeline that generates synthetic sales data and lands it in GCS. The data generator runs as a Flask app on Cloud Run, built and deployed via Cloud Build CI/CD. The project is being built incrementally — the data generation + Cloud Run layer is complete; PySpark/Dataproc processing, BigQuery loading, and Airflow orchestration will be added progressively.

## Current components

| File | Purpose |
|---|---|
| `data/data_generator.py` | Synthetic data generator wrapped as a Flask app (`/health`, `/generate`) |
| `data/Dockerfile` | Container image — gunicorn serving the Flask app on port 8080 |
| `data/cloudbuild.yaml` | Cloud Build pipeline: build image → push to Artifact Registry → deploy to Cloud Run |

## Running locally

```bash
pip install -r requirements.txt
python data/data_generator.py
```

Outputs five date-stamped CSV files into `synthetic_data/` (created automatically).

To run as a server and trigger via API:

```bash
GCS_BUCKET=my-bucket python data/data_generator.py
curl -X POST http://localhost:8080/generate
```

## Docker

Build context is the `data/` directory:

```bash
docker build -f data/Dockerfile -t data-generator .
docker run -p 8080:8080 -e GCS_BUCKET=my-bucket data-generator
```

## Cloud Build / Cloud Run

`data/cloudbuild.yaml` contains placeholder values — fill them in before triggering a build:

- `YOUR_PROJECT_ID` — GCP project ID
- `YOUR_GCS_BUCKET` — GCS bucket for output and Cloud Build logs
- `YOUR_AR_REPO_NAME` — Artifact Registry Docker repository name
- `YOUR_SERVICE_ACCOUNT_NAME` — service account used by the Cloud Run service (needs `roles/storage.objectCreator` on the bucket)

## Data schema

Output files are date-stamped (`customers_YYYYMMDD.csv`, etc.).

| File | Key columns |
|---|---|
| `customers` | `customer_id`, `customer_name`, `email`, `signup_date`, `region`, `customer_segment` |
| `products` | `product_id`, `product_name`, `category`, `sub_category`, `brand`, `unit_price` |
| `orders` | `order_id`, `customer_id`, `order_date`, `order_status`, `total_amount`, `discount_amount`, `payment_method` |
| `order_items` | `order_item_id`, `order_id`, `product_id`, `quantity`, `price_per_unit` |
| `transactions` | `transaction_id`, `order_id`, `transaction_date`, `transaction_amount`, `transaction_status` |

`order_status`: `completed` (70%), `shipped` (20%), `returned` (5%), `cancelled` (5%).  
`transaction_status`: `success` (95%), `failed` (5%).
