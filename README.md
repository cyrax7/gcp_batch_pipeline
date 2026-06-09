# GCP Batch Data Pipeline

## Project Overview

This project implements a production-style batch data pipeline on Google Cloud Platform (GCP). A Cloud Run service generates synthetic sales data daily, writes it to Cloud Storage, and an Airflow DAG (Cloud Composer) orchestrates a Dataproc PySpark job that transforms and loads the data into BigQuery. A CI/CD pipeline via Cloud Build automates image builds, deployments, and artifact uploads on every code push.

The pipeline processes five raw CSV datasets — customers, products, orders, order items, and transactions — and produces six analytical BigQuery tables covering order summaries, monthly sales trends, customer profiles, transaction history, and retention metrics. These tables are ready for visualisation in Looker Studio or Power BI.

---

## Architecture Overview

```
Cloud Scheduler (daily trigger)
        │
        ▼
  Cloud Run Service          ← Generates synthetic sales CSVs
        │
        ▼
  Cloud Storage (GCS)        ← Stores raw CSV data, PySpark scripts, and config
        │
        ▼
  Cloud Composer (Airflow)   ← Orchestrates the pipeline
        │
        ▼
  Cloud Dataproc (PySpark)   ← Transforms and aggregates data
        │
        ▼
  BigQuery                   ← Stores transformed analytical tables
        │
        ▼
  Looker Studio / Power BI   ← Dashboards and visualizations
```

| Component | Role |
|---|---|
| **Cloud Run** | Flask app that generates synthetic sales data and writes CSVs to GCS |
| **Cloud Storage** | Source for raw CSVs; also stores PySpark scripts and pipeline config |
| **Cloud Dataproc** | Runs PySpark transformation jobs on an ephemeral cluster |
| **BigQuery** | Destination for all transformed and aggregated tables |
| **Cloud Composer** | Airflow-based orchestrator — schedules, monitors, and sequences all tasks |
| **Cloud Build** | CI/CD — builds, pushes, and deploys on every code push |
| **Looker Studio / Power BI** | Dashboard and visualization layer on top of BigQuery |

---

## GCP Resources

Configure these values in `config/config.json` before deploying. The same file is uploaded to GCS and read by the Airflow DAG at parse time.

| Resource | Config Key |
|---|---|
| Project ID | `project_id` |
| Region | `region` |
| Zone | `zone` |
| GCS Bucket | `gcs_bucket` |
| BigQuery Dataset | `bq_dataset` |
| Dataproc Cluster | `cluster_name` |
| Composer Environment | set via `_COMPOSER_DAGS_BUCKET` in `cloudbuild.yaml` |
| Service Account | set via `_SERVICE_ACCOUNT` in `cloudbuild.yaml` |
| VPC Subnetwork | `subnetwork` |

---

## Prerequisites

- Google Cloud account with permissions to manage Cloud Storage, Dataproc, BigQuery, Cloud Run, and Cloud Composer
- `gcloud` CLI installed and authenticated
- Python 3.9+ (for local testing)
- Docker (for building the data generator image locally)
- Power BI Desktop (Windows) or access to Looker Studio for dashboards

---

## Repository Structure

```
.
├── airflow/
│   └── sales_pipeline_dag.py       # Airflow DAG — orchestrates the full pipeline
├── config/
│   └── config.json                 # Pipeline config (uploaded to GCS on every build)
├── data_generator/
│   ├── main.py                     # Flask app — generates synthetic CSV data
│   ├── Dockerfile
│   └── requirements.txt
├── pyspark/
│   ├── utils.py                    # Shared helpers (parser, Spark session, BQ writer)
│   ├── run_all.py                  # Production entrypoint — runs all 6 transforms
│   ├── order_summary.py
│   ├── monthly_sales.py
│   ├── monthly_performance.py
│   ├── customer_profile_summary.py
│   ├── customer_transactions.py
│   └── customer_retention.py
├── cloudbuild.yaml                 # CI/CD — build, push, deploy, upload artifacts
├── CLAUDE.md                       # Project context for Claude Code
└── README.md
```

---

## Pipeline Outputs

### Raw data (GCS)

The data generator writes five CSV files to GCS on every run:

```
gs://<gcs_bucket>/<gcs_prefix>/
    customers.csv        — 1,000 customers
    products.csv         — 500 products
    orders.csv           — 5,000 orders
    order_items.csv      — 2,000 order items
    transactions.csv     — 5,000 transactions
```

Files are overwritten on every run (no date suffix). The `ingestion_date` partition column in BigQuery tracks when each batch was processed.

### Analytical tables (BigQuery)

All six tables are written to the configured BigQuery dataset in **append mode**, partitioned by `ingestion_date` (DAY):

| Table | Description |
|---|---|
| `order_summary` | Per-order totals, status, and payment breakdown |
| `monthly_sales` | Revenue by year/month/region/category |
| `monthly_performance` | Order volume and revenue trends by month |
| `customer_profile_summary` | Lifetime value and segment per customer |
| `customer_transactions` | Transaction history linked to orders |
| `customer_retention` | New vs returning customer analysis |

---

## Pipeline Flow

### Step 1 — Data Generation (Cloud Run)

A Flask app deployed on Cloud Run generates synthetic sales data on a daily schedule triggered by Cloud Scheduler at **01:00 IST**.

`POST /generate` produces the 5 CSV files listed above, written directly to GCS.

### Step 2 — Orchestration (Airflow DAG)

The Airflow DAG (`sales_pipeline_dag`) runs at **20:30 UTC (02:00 IST)** daily with this task graph:

```
sense_source_data_in_gcs
        │
        ▼
create_dataproc_cluster
        │
        ▼
run_all_transforms          ← single Dataproc job running all 6 PySpark transforms
        │
        ▼
delete_dataproc_cluster
```

- **GCS Sensor** — waits for the configured sensor file (e.g. `orders.csv`) to confirm the data generator completed
- **Create Cluster** — ephemeral Dataproc cluster (`use_if_exists=True` to handle reruns)
- **Run All Transforms** — submits `run_all.py` as one Dataproc job (all 6 transforms in a single Spark session)
- **Delete Cluster** — always runs (`trigger_rule=ALL_DONE`) to avoid cost leakage

All operators use `deferrable=True` to prevent Airflow worker heartbeat timeouts during long Dataproc waits.

DAG configuration (cluster name, bucket, schedule, etc.) is loaded at parse time from `gs://<gcs_bucket>/config/config.json`. The GCS path is set via the `GCS_CONFIG_PATH` environment variable in the Composer environment.

### Step 3 — PySpark Transformations (Dataproc)

`run_all.py` reads and cleans all 5 CSVs once, then runs all 6 transforms in a single Spark session, writing results to the six BigQuery tables listed above.

---

## CI/CD — Cloud Build

Every push to the repository triggers `cloudbuild.yaml` which runs these phases in parallel where possible:

| Phase | What it does |
|---|---|
| 1 — Build | Docker build for the data generator image |
| 2 — Push | Push image to Artifact Registry |
| 3 — Deploy | Deploy updated image to Cloud Run |
| 4 — Upload PySpark | Copy all `pyspark/*.py` files to GCS |
| 5 — Upload DAG | Copy `airflow/sales_pipeline_dag.py` to Composer DAGs bucket |
| 6 — Upload Config | Copy `config/config.json` to GCS |

Before triggering a build, set the substitution variables in `cloudbuild.yaml`:

```yaml
substitutions:
  _REGION: "YOUR_GCP_REGION"
  _PROJECT_ID: "YOUR_GCP_PROJECT_ID"
  _GCS_BUCKET: "YOUR_GCS_BUCKET"
  _SERVICE_ACCOUNT: "YOUR_SERVICE_ACCOUNT_NAME"
  _BQ_DATASET: "YOUR_BQ_DATASET"
  _COMPOSER_DAGS_BUCKET: "gs://YOUR_COMPOSER_BUCKET/dags"
```

---

## Networking

This project uses a custom VPC. All resources (Cloud Run, Dataproc, Composer) should be configured to use the same VPC and subnet, set via the `subnetwork` key in `config.json`.

Two firewall rules are required for Dataproc VM-to-VM communication:

| Rule | Direction | Range | Action |
|---|---|---|---|
| `allow-dataproc-internal` | Ingress | `10.0.0.0/8` | Allow all |
| `allow-dataproc-internal-egress` | Egress | `10.0.0.0/8` | Allow all |

Both rules are required — if the VPC has a default-deny egress rule, workers cannot communicate back to the master without the egress allow rule.

---

## Dashboards

### Option 1 — Looker Studio (free, browser-based)

1. Open a BigQuery table in [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)
2. Click **Export → Explore with Looker Studio**
3. Add remaining tables via **Resource → Manage added data sources**
4. Build charts using the 6 BigQuery tables

> Note: Looker Studio may be restricted by your organisation's Google Workspace settings.

### Option 2 — Power BI Desktop (free, Windows)

1. Download and install [Power BI Desktop](https://powerbi.microsoft.com/desktop)
2. **Get Data → Google BigQuery** → sign in with your Google account
3. Select your project → your BigQuery dataset → load all 6 tables
4. Build charts and click **Refresh** to pull the latest data

Suggested visualizations:

| Table | Chart | Key fields |
|---|---|---|
| `monthly_sales` | Line chart | x=month, y=gross_revenue, legend=region |
| `monthly_performance` | Bar chart | x=month, y=total_orders |
| `order_summary` | Scorecard | total_orders, gross_revenue, avg_order_value |
| `customer_profile_summary` | Table | top customers ranked by lifetime value |
| `customer_transactions` | Time series | transaction_date, transaction_amount |
| `customer_retention` | Pie / bar | returning vs new customers |

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| DAG shows `404` for `config.json` | Config not uploaded to GCS | Run `gsutil cp config/config.json gs://YOUR_GCS_BUCKET/config/config.json` |
| Dataproc cluster fails with "VM to VM communications blocked" | Missing egress firewall rule | Create `allow-dataproc-internal-egress` (Egress, dest `10.0.0.0/8`, allow all) |
| Airflow tasks fail before submitting to Dataproc | Too many concurrent deferrable tasks overwhelming the worker | Use a single `run_all.py` job instead of 6 parallel tasks |
| Cloud Build step fails with 401 | Auth not injected in parallel step | Use `gcr.io/cloud-builders/gcloud` with `gcloud storage cp` instead of `gsutil` |
| PySpark `FileNotFoundError` for CSV | Script reading date-suffixed filenames | All CSVs use fixed names (`customers.csv`, not `customers_YYYYMMDD.csv`) |
| Airflow task killed mid-run with SQL heartbeat error | Cloud SQL proxy connection drop during long Dataproc wait | Ensure all operators have `deferrable=True` |

---

## Key Design Decisions

- **Single Dataproc job** — `run_all.py` runs all 6 transforms in one Spark session rather than 6 parallel Airflow tasks. This avoids Airflow worker concurrency limits that caused tasks to fail before submitting.
- **Deferrable operators** — all Dataproc operators use `deferrable=True` so the Airflow worker releases its slot during long waits, preventing Cloud SQL proxy heartbeat timeouts.
- **GCS-backed config** — `config/config.json` is loaded by the DAG at parse time from GCS, so infrastructure values (cluster name, bucket, schedule, etc.) can be changed without editing the DAG code.
- **Ephemeral cluster** — cluster is created at the start of each DAG run and deleted at the end (`trigger_rule=ALL_DONE`), minimising idle compute cost.
- **Fixed CSV filenames** — the data generator overwrites the same 5 files on every run. The `ingestion_date` column in BigQuery tracks batch dates instead.
