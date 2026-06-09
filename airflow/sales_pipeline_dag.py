import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

from google.cloud import storage

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Load config from GCS
# ---------------------------------------------------------------------------

def _read_gcs_config(gcs_path):
    parsed = urlparse(gcs_path)
    client = storage.Client()
    blob = client.bucket(parsed.netloc).blob(parsed.path.lstrip("/"))
    return json.loads(blob.download_as_text())


# Set GCS_CONFIG_PATH env var to the full GCS URI of config.json, e.g.:
#   gs://<your-bucket>/config/config.json
config = _read_gcs_config(os.environ["GCS_CONFIG_PATH"])

PROJECT_ID      = config["project_id"]
REGION          = config["region"]
ZONE            = config["zone"]
CLUSTER_NAME    = config["cluster_name"]
GCS_BUCKET      = config["gcs_bucket"]
GCS_PREFIX      = config["gcs_prefix"]
BQ_DATASET      = config["bq_dataset"]
SUBNETWORK      = config["subnetwork"]
SERVICE_ACCOUNT = config["service_account"]
PYSPARK_RUN_ALL = config["pyspark_run_all"]
PYSPARK_PY_FILES = config["pyspark_py_files"]

# ---------------------------------------------------------------------------
# Cluster and job config
# ---------------------------------------------------------------------------

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": config["master_num_instances"],
        "machine_type_uri": config["master_machine_type"],
        "disk_config": {"boot_disk_size_gb": config["master_disk_size_gb"]},
    },
    "worker_config": {
        "num_instances": config["worker_num_instances"],
        "machine_type_uri": config["worker_machine_type"],
        "disk_config": {"boot_disk_size_gb": config["worker_disk_size_gb"]},
    },
    "software_config": {
        "image_version": config["dataproc_image_version"],
        "properties": {
            "spark:spark.jars.packages": config["spark_bq_package"],
        },
    },
    "gce_cluster_config": {
        "zone_uri": ZONE,
        "subnetwork_uri": SUBNETWORK,
        "service_account": SERVICE_ACCOUNT,
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "internal_ip_only": False,
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_RUN_ALL,
        "python_file_uris": PYSPARK_PY_FILES,
        "args": [
            f"--gcs-bucket={GCS_BUCKET}",
            f"--gcs-prefix={GCS_PREFIX}",
            f"--bq-project={PROJECT_ID}",
            f"--bq-dataset={BQ_DATASET}",
            f"--bq-temp-bucket={GCS_BUCKET}",
        ],
    },
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": config["dag_owner"],
    "retries": config["dag_retries"],
    "retry_delay": timedelta(minutes=config["dag_retry_delay_minutes"]),
    "email_on_failure": False,
}

with DAG(
    dag_id="sales_pipeline_dag",
    description="Daily batch pipeline: GCS → Dataproc PySpark → BigQuery",
    schedule_interval=config["dag_schedule_interval"],
    start_date=datetime.strptime(config["dag_start_date"], "%Y-%m-%d"),
    catchup=False,
    default_args=default_args,
    tags=["dataproc", "pyspark", "bigquery", "sales"],
) as dag:

    # 1. Wait for orders CSV to confirm data generator has finished
    sense_source_data = GCSObjectsWithPrefixExistenceSensor(
        task_id="sense_source_data_in_gcs",
        bucket=GCS_BUCKET,
        prefix=f"{GCS_PREFIX}/{config['sensor_file']}",
        mode="reschedule",
        poke_interval=config["sensor_poke_interval"],
        timeout=config["sensor_timeout"],
    )

    # 2. Create cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        use_if_exists=True,
        deferrable=True,
    )

    # 3. Single PySpark job — runs all 6 transforms in one Spark session
    run_all_transforms = DataprocSubmitJobOperator(
        task_id="run_all_transforms",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        deferrable=True,
    )

    # 4. Delete cluster — always runs even if transforms fail
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )

    sense_source_data >> create_cluster >> run_all_transforms >> delete_cluster
