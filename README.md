# GCP Batch Data Pipeline

A GCP batch pipeline that generates synthetic sales data daily and lands it in Google Cloud Storage. Built to run as a containerised Flask application deployed as a GKE CronJob, with CI/CD through Cloud Build.

---

## Architecture

```
Cloud Build (CI/CD)
       │
       ▼
Artifact Registry (Docker image)
       │
       ▼
GKE CronJob (runs daily)
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
├── k8s/
│   ├── deployment.yaml       # GKE CronJob manifest
│   └── service.yaml          # Kubernetes ClusterIP service
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

The Dockerfile uses the repo root as build context so it can reach both `requirements.txt` and `data/data_generator.py`.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data/data_generator.py main.py
EXPOSE 8080
CMD ["python", "main.py"]
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

Triggered on every push. Runs three sequential phases.

### Phase 1 — Build & Push

Builds the Docker image from `data/Dockerfile` with the repo root as context and pushes it to Artifact Registry tagged with `$SHORT_SHA`.

```
us-central1-docker.pkg.dev/<PROJECT_ID>/<REPO>/gcp-batch-pipeline-data-generator:<SHORT_SHA>
```

### Phase 2 — Prepare Manifests

Runs `sed` across all `k8s/*.yaml` files to replace placeholders with live values before deployment.

| Placeholder | Replaced with |
|---|---|
| `{{SHORT_SHA}}` | Git commit short SHA |
| `{{CRONJOB_IMAGE_URI}}` | Full Artifact Registry image path |
| `{{GCS_BUCKET}}` | Value of `_GCS_BUCKET` substitution |
| `{{GCS_PREFIX}}` | Value of `_GCS_PREFIX` substitution |

### Phase 3 — Deploy to GKE

Runs `kubectl apply -f k8s/` against the configured cluster.

### Required Substitutions

Before triggering a build, fill in the following values in `data/cloudbuild.yaml`:

```yaml
substitutions:
  _REGION: "us-central1"
  _PROJECT_ID: "<YOUR_PROJECT_ID>"
  _REPO: "<YOUR_AR_REPO_NAME>"
  _CLUSTER: "<YOUR_GKE_CLUSTER_NAME>"
  _GCS_BUCKET: "<YOUR_GCS_BUCKET_NAME>"
  _GCS_PREFIX: "synthetic_data"
```

Also update the top-level `logsBucket` and `serviceAccount` fields.

---

## Kubernetes Resources

### CronJob (`k8s/deployment.yaml`)

| Field | Value |
|---|---|
| Schedule | `0 1 * * *` (daily at 01:00 UTC) |
| Concurrency | `Forbid` (no overlapping runs) |
| Restart policy | `OnFailure` |
| Backoff limit | 2 retries |
| CPU request / limit | 250m / 500m |
| Memory request / limit | 512Mi / 1Gi |
| Service account | `data-generator-ksa` |

The `data-generator-ksa` Kubernetes service account must be bound to a GCP service account with `roles/storage.objectCreator` on the target GCS bucket via Workload Identity.

### Service (`k8s/service.yaml`)

`ClusterIP` service that routes port `80` to the Flask container's port `8080`. Allows other pods in the cluster to call `/health` and `/generate` without exposing the app externally.

---

## GCP Permissions Required

| Principal | Role | Scope |
|---|---|---|
| Cloud Build service account | `roles/container.developer` | GKE cluster |
| Cloud Build service account | `roles/artifactregistry.writer` | Artifact Registry repo |
| `data-generator-ksa` (via Workload Identity) | `roles/storage.objectCreator` | GCS bucket |
