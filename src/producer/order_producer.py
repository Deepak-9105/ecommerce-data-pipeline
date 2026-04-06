import json
import random
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

fake = Faker('en_IN')  # Indian locale for realistic data

# ── Configuration ────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME   = "orders_topic"
ORDERS_PER_SECOND = 5

# ── Indian cities and products ───────────────────────────────────
CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Surat"
]

PRODUCTS = [
    {"id": "P001", "name": "iPhone 15",        "price": 79999},
    {"id": "P002", "name": "Samsung Galaxy S24","price": 54999},
    {"id": "P003", "name": "Nike Air Max",      "price": 8999},
    {"id": "P004", "name": "Levi's Jeans",      "price": 2999},
    {"id": "P005", "name": "Boat Earphones",    "price": 1499},
    {"id": "P006", "name": "Instant Pot",       "price": 6999},
    {"id": "P007", "name": "IKEA Table Lamp",   "price": 1299},
    {"id": "P008", "name": "Protein Powder",    "price": 2499},
    {"id": "P009", "name": "Yoga Mat",          "price": 799},
    {"id": "P010", "name": "Cricket Bat",       "price": 3499},
]

ORDER_STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]


# ── Order Generator ──────────────────────────────────────────────
def generate_order():
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    return {
        "order_id":       f"ORD-{fake.unique.random_int(min=100000, max=999999)}",
        "user_id":        f"USR-{random.randint(1000, 9999)}",
        "product_id":     product["id"],
        "product_name":   product["name"],
        "quantity":       quantity,
        "unit_price":     product["price"],
        "total_amount":   product["price"] * quantity,
        "city":           random.choice(CITIES),
        "status":         random.choice(ORDER_STATUSES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "customer_name":  fake.name(),
        "timestamp":      datetime.now().isoformat(),
    }


# ── Kafka Producer ───────────────────────────────────────────────
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def on_success(metadata):
    print(f"  ✅ Sent to topic={metadata.topic} "
          f"partition={metadata.partition} "
          f"offset={metadata.offset}")


def on_error(e):
    print(f"  ❌ Error sending message: {e}")


# ── Main Loop ────────────────────────────────────────────────────
def main():
    print(f"🚀 Starting Order Producer → topic: {TOPIC_NAME}")
    print(f"   Sending {ORDERS_PER_SECOND} orders/second. Press Ctrl+C to stop.\n")

    producer = create_producer()
    total_sent = 0

    try:
        while True:
            for _ in range(ORDERS_PER_SECOND):
                order = generate_order()
                producer.send(
                    TOPIC_NAME,
                    key=order["order_id"],
                    value=order
                ).add_callback(on_success).add_errback(on_error)
                total_sent += 1

            producer.flush()
            print(f"📦 Total orders sent: {total_sent} | "
                  f"Last order: {order['product_name']} "
                  f"from {order['city']} | "
                  f"₹{order['total_amount']:,}")
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n⛔ Producer stopped. Total orders sent: {total_sent}")
        producer.close()


if __name__ == "__main__":
    main()