import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Configuration
NUM_ORDERS = 1000
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orders.csv")

# Sample data
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "USB Cable"]
categories = ["Electronics", "Accessories"]
payment_types = ["Credit Card", "PayPal", "Debit Card"]
countries = ["USA", "Canada", "UK", "Germany", "France"]

# Generate data
data = []
start_date = datetime(2025, 1, 1)

for i in range(1, NUM_ORDERS + 1):
    order_date = start_date + timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    row = {
        "order_id": i,
        "customer_id": random.randint(1, 500),
        "product_name": random.choice(products),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 2000), 2),
        "quantity": random.randint(1, 5),
        "order_date": order_date,
        "payment_type": random.choice(payment_types),
        "country": random.choice(countries)
    }
    data.append(row)

# Create DataFrame
df = pd.DataFrame(data)

# Save CSV
df.to_csv(OUTPUT_FILE, index=False)
print(f"{NUM_ORDERS} simulated orders generated at {OUTPUT_FILE}")
