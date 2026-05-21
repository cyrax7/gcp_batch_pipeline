# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

GCP batch data pipeline that generates synthetic sales data and processes it through GCS → Dataproc/PySpark → BigQuery, orchestrated by Airflow on Cloud Composer. The project is being built incrementally — `data/data_generator.py` is the starting point; PySpark jobs, Airflow DAG, and CI/CD will be added progressively.

## Running the data generator

```bash
pip install -r requirements.txt
python data/data_generator.py
```

Outputs five CSV files into `synthetic_data/` (created automatically): `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `transactions.csv`.

## Data schema

| File | Key columns |
|---|---|
| `customers.csv` | `customer_id`, `customer_name`, `email`, `signup_date`, `region`, `customer_segment` |
| `products.csv` | `product_id`, `product_name`, `category`, `sub_category`, `brand`, `unit_price` |
| `orders.csv` | `order_id`, `customer_id`, `order_date`, `order_status`, `total_amount`, `discount_amount`, `payment_method` |
| `order_items.csv` | `order_item_id`, `order_id`, `product_id`, `quantity`, `price_per_unit` |
| `transactions.csv` | `transaction_id`, `order_id`, `transaction_date`, `transaction_amount`, `transaction_status` |

`order_status` values: `completed` (70%), `shipped` (20%), `returned` (5%), `cancelled` (5%).  
`transaction_status` values: `success` (95%), `failed` (5%).
