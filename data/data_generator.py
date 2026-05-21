import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

NUM_CUSTOMERS = 100
NUM_PRODUCTS = 500
NUM_ORDERS = 500
NUM_ORDER_ITEMS = 2000
NUM_TRANSACTIONS = 500

OUTPUT_DIR = "synthetic_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


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
    df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"Generated {n} customers.")
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
    df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
    print(f"Generated {n} products.")
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
    df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    print(f"Generated {n} orders.")
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
    df.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)
    print(f"Generated {n} order items.")
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
    df.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
    print(f"Generated {n} transactions.")
    return df


if __name__ == "__main__":
    customers_df = generate_customers()
    products_df = generate_products()
    orders_df = generate_orders(customers=customers_df)
    order_items_df = generate_order_items(orders=orders_df, products=products_df)
    transactions_df = generate_transactions(orders=orders_df)
    print(f"All data written to ./{OUTPUT_DIR}/")
