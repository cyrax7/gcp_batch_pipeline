import logging
import os
import random
import sys
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from flask import Flask, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

fake = Faker()
app = Flask(__name__)

NUM_CUSTOMERS = 100
NUM_PRODUCTS = 500
NUM_ORDERS = 500
NUM_ORDER_ITEMS = 2000
NUM_TRANSACTIONS = 500

TODAY = datetime.now().strftime("%Y%m%d")

_GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
_GCS_PREFIX = os.environ.get("GCS_PREFIX", "synthetic_data")

if _GCS_BUCKET:
    BASE_PATH = f"gs://{_GCS_BUCKET}/{_GCS_PREFIX}"
    log.info("Output destination: %s/", BASE_PATH)
else:
    BASE_PATH = os.environ.get("OUTPUT_DIR", "synthetic_data")
    os.makedirs(BASE_PATH, exist_ok=True)
    log.info("Output destination: %s/ (local)", BASE_PATH)


def random_date(start, end):
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def generate_customers(n=NUM_CUSTOMERS):
    segments = ["Regular", "Premium", "VIP", "Loyal", "Occasional"]
    regions = [
        "North America", "Europe", "Asia", "South America",
        "Australia", "Africa", "Middle East", "Southeast Asia",
    ]
    rows = [
        [
            fake.uuid4(),
            fake.name(),
            fake.email(),
            random_date(datetime(2018, 1, 1), datetime(2024, 10, 1)),
            random.choice(regions),
            random.choice(segments),
        ]
        for _ in range(n)
    ]
    df = pd.DataFrame(rows, columns=[
        "customer_id", "customer_name", "email",
        "signup_date", "region", "customer_segment",
    ])
    df.to_csv(f"{BASE_PATH}/customers_{TODAY}.csv", index=False)
    log.info("Generated %d customers.", n)
    return df


def generate_products(n=NUM_PRODUCTS):
    categories = {
        "Electronics": ["Mobile Phones", "Laptops", "Accessories", "Cameras", "TVs"],
        "Clothing": ["Men", "Women", "Kids", "Accessories", "Footwear"],
        "Home & Kitchen": ["Appliances", "Furniture", "Kitchenware", "Bedding", "Decor"],
        "Books": ["Fiction", "Non-Fiction", "Comics", "Textbooks", "E-books"],
        "Toys": ["Action Figures", "Dolls", "Educational", "Board Games", "Outdoor"],
        "Sports": ["Fitness", "Outdoor Gear", "Team Sports", "Cycling", "Running"],
        "Beauty": ["Skincare", "Makeup", "Haircare", "Fragrances", "Nail Care"],
        "Automotive": ["Car Accessories", "Motorcycle", "Tools", "Electronics", "Parts"],
    }
    weights = [0.30, 0.20, 0.20, 0.10, 0.08, 0.06, 0.04, 0.02]
    cat_list = list(categories.keys())
    rows = []
    for _ in range(n):
        cat = random.choices(cat_list, weights=weights)[0]
        rows.append([
            fake.uuid4(),
            fake.word().capitalize() + " " + fake.word().capitalize(),
            cat,
            random.choice(categories[cat]),
            fake.company(),
            round(random.uniform(5, 1500), 2),
        ])
    df = pd.DataFrame(rows, columns=[
        "product_id", "product_name", "category",
        "sub_category", "brand", "unit_price",
    ])
    df.to_csv(f"{BASE_PATH}/products_{TODAY}.csv", index=False)
    log.info("Generated %d products.", n)
    return df


def generate_orders(n=NUM_ORDERS, customers=None):
    statuses = ["completed", "shipped", "returned", "cancelled"]
    status_weights = [0.70, 0.20, 0.05, 0.05]
    payment_methods = ["credit_card", "paypal", "cash", "bank_transfer", "gift_card"]
    cids = customers["customer_id"].tolist()
    rows = [
        [
            fake.uuid4(),
            random.choice(cids),
            random_date(datetime(2019, 1, 1), datetime(2024, 12, 31)),
            random.choices(statuses, weights=status_weights)[0],
            round(random.uniform(5, 1000), 2),
            round(random.uniform(0, 50), 2),
            random.choice(payment_methods),
        ]
        for _ in range(n)
    ]
    df = pd.DataFrame(rows, columns=[
        "order_id", "customer_id", "order_date", "order_status",
        "total_amount", "discount_amount", "payment_method",
    ])
    df.to_csv(f"{BASE_PATH}/orders_{TODAY}.csv", index=False)
    log.info("Generated %d orders.", n)
    return df


def generate_order_items(n=NUM_ORDER_ITEMS, orders=None, products=None):
    oids = orders["order_id"].tolist()
    pids = products["product_id"].tolist()
    rows = [
        [
            fake.uuid4(),
            random.choice(oids),
            random.choice(pids),
            random.randint(1, 5),
            round(random.uniform(1, 300), 2),
        ]
        for _ in range(n)
    ]
    df = pd.DataFrame(rows, columns=[
        "order_item_id", "order_id", "product_id", "quantity", "price_per_unit",
    ])
    df.to_csv(f"{BASE_PATH}/order_items_{TODAY}.csv", index=False)
    log.info("Generated %d order items.", n)
    return df


def generate_transactions(n=NUM_TRANSACTIONS, orders=None):
    statuses = ["success", "failed"]
    weights = [0.95, 0.05]
    oids = orders["order_id"].tolist()
    rows = [
        [
            fake.uuid4(),
            random.choice(oids),
            random_date(datetime(2019, 1, 1), datetime(2024, 12, 31)),
            round(random.uniform(5, 1000), 2),
            random.choices(statuses, weights=weights)[0],
        ]
        for _ in range(n)
    ]
    df = pd.DataFrame(rows, columns=[
        "transaction_id", "order_id", "transaction_date",
        "transaction_amount", "transaction_status",
    ])
    df.to_csv(f"{BASE_PATH}/transactions_{TODAY}.csv", index=False)
    log.info("Generated %d transactions.", n)
    return df


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


@app.route("/generate", methods=["POST"])
def generate():
    try:
        customers_df = generate_customers()
        products_df = generate_products()
        orders_df = generate_orders(customers=customers_df)
        generate_order_items(orders=orders_df, products=products_df)
        generate_transactions(orders=orders_df)
        log.info("All data written to %s/ (date suffix: %s)", BASE_PATH, TODAY)
        return jsonify({"status": "success", "destination": BASE_PATH, "date": TODAY}), 200
    except Exception as e:
        log.exception("Data generation failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    customers_df = generate_customers()
    products_df = generate_products()
    orders_df = generate_orders(customers=customers_df)
    generate_order_items(orders=orders_df, products=products_df)
    generate_transactions(orders=orders_df)
    log.info("All data written to %s/ (date suffix: %s)", BASE_PATH, TODAY)
