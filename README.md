# GCP Batch Data Pipeline

A GCP batch pipeline that generates synthetic sales data daily and lands it in Google Cloud Storage. Built to run as a containerised Flask application deployed on Cloud Run, with CI/CD through Cloud Build.

---

## Architecture

```
Cloud Build (CI/CD)
       │
       ▼
Artifact Registry (Docker image)
       │
       ▼
Cloud Run (invoked via POST /generate)
       │
       ▼
Flask App  ──── POST /generate ────▶  Data Generator
                                            │
                                            ▼
                               GCS Bucket (CSV files)
                          synthetic_data/customers_YYYYMMDD.csv
                          synthetic_data/products_YYYYMMDD.csv
                          synthetic_data/orders_YYYYMMDD.csv
                          synthetic_data/order_items_YYYYMMDD.csv
                          synthetic_data/transactions_YYYYMMDD.csv
```

---

## Repository Structure

```
gcp_batch_pipeline/
│
├── data/
│   ├── data_generator.py     # Synthetic data generator + Flask app
│   ├── Dockerfile            # Container image definition
│   └── cloudbuild.yaml       # Cloud Build CI/CD pipeline
│
├── requirements.txt
└── README.md
```

---

## Data Generator (`data/data_generator.py`)

Generates five interlinked synthetic CSV files using `faker` and `pandas`. Each file is date-stamped with the run date (`YYYYMMDD`) so daily runs never overwrite each other.

### Generated Files

| File | Rows | Key Columns |
|---|---|---|
| `customers_YYYYMMDD.csv` | 100 | `customer_id`, `customer_name`, `email`, `signup_date`, `region`, `customer_segment` |
| `products_YYYYMMDD.csv` | 500 | `product_id`, `product_name`, `category`, `sub_category`, `brand`, `unit_price` |
| `orders_YYYYMMDD.csv` | 500 | `order_id`, `customer_id`, `order_date`, `order_status`, `total_amount`, `discount_amount`, `payment_method` |
| `order_items_YYYYMMDD.csv` | 2000 | `order_item_id`, `order_id`, `product_id`, `quantity`, `price_per_unit` |
| `transactions_YYYYMMDD.csv` | 500 | `transaction_id`, `order_id`, `transaction_date`, `transaction_amount`, `transaction_status` |

### Data Distributions

- `order_status`: `completed` (70%), `shipped` (20%), `returned` (5%), `cancelled` (5%)
- `transaction_status`: `success` (95%), `failed` (5%)

### Output Destination

| `GCS_BUCKET` set? | Output |
|---|---|
| Yes | `gs://<GCS_BUCKET>/<GCS_PREFIX>/` |
| No | Local `./synthetic_data/` directory |

---

## Flask API

The generator is wrapped in a Flask app exposing two endpoints.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Liveness/readiness probe. Returns `{"status": "healthy"}`. |
| `POST` | `/generate` | Runs the full data generation pipeline. Returns destination path and run date. |

### `/generate` Response

```json
{
  "status": "success",
  "destination": "gs://my-bucket/synthetic_data",
  "date": "20260521"
}
```

---

## Running Locally

```bash
pip install -r requirements.txt
python data/data_generator.py
```

CSV files are written to `./synthetic_data/`.

To run as a Flask server locally:

```bash
GCS_BUCKET=my-bucket python data/data_generator.py
# Then trigger generation:
curl -X POST http://localhost:8080/generate
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCS_BUCKET` | No | _(local mode)_ | GCS bucket name. Enables GCS output when set. |
| `GCS_PREFIX` | No | `synthetic_data` | Path prefix inside the GCS bucket. |
| `OUTPUT_DIR` | No | `synthetic_data` | Local output directory (used when `GCS_BUCKET` is not set). |

---

## Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data_generator.py main.py
EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "--workers", "1", "main:app"]
```

### Build and run locally

```bash
docker build -f data/Dockerfile -t data-generator .

docker run -p 8080:8080 \
  -e GCS_BUCKET=my-bucket \
  -e GCS_PREFIX=synthetic_data \
  data-generator
```

---

## Cloud Build CI/CD (`data/cloudbuild.yaml`)

Triggered on every push. Runs two sequential phases.

### Phase 1 — Build & Push

Builds the Docker image from `data/Dockerfile` and pushes it to Artifact Registry tagged with `$SHORT_SHA`.

```
us-central1-docker.pkg.dev/<PROJECT_ID>/<REPO>/gcp-batch-pipeline-data-generator:<SHORT_SHA>
```

### Phase 2 — Deploy to Cloud Run

Deploys the image to Cloud Run with the following configuration:

| Setting | Value |
|---|---|
| Platform | managed |
| Authentication | `--no-allow-unauthenticated` |
| Memory | 1Gi |
| CPU | 1 |
| Timeout | 540s |
| Max instances | 1 |
| Concurrency | 1 |

### Configuration

Before triggering a build, fill in the placeholders in `data/cloudbuild.yaml`:

```yaml
logsBucket: gs://YOUR_GCS_BUCKET/cloudbuild-logs
serviceAccount: projects/YOUR_PROJECT_ID/serviceAccounts/YOUR_SERVICE_ACCOUNT_NAME@YOUR_PROJECT_ID.iam.gserviceaccount.com

substitutions:
  _REGION: "us-central1"
  _PROJECT_ID: "YOUR_PROJECT_ID"
  _REPO: "YOUR_AR_REPO_NAME"
  _SERVICE_NAME: "gcp-batch-pipeline-data-generator"
  _GCS_BUCKET: "YOUR_GCS_BUCKET"
  _GCS_PREFIX: "synthetic_data"
```

Also replace `YOUR_SERVICE_ACCOUNT_NAME` in the `--service-account` flag of the deploy step.

---

## GCP Permissions Required

| Principal | Role | Scope |
|---|---|---|
| Cloud Build service account | `roles/run.admin` | Cloud Run service |
| Cloud Build service account | `roles/artifactregistry.writer` | Artifact Registry repo |
| Cloud Build service account | `roles/iam.serviceAccountUser` | Cloud Run runtime service account |
| Cloud Run runtime service account | `roles/storage.objectCreator` | GCS bucket |
