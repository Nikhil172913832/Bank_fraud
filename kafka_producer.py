import random
import time
import msgpack
import numpy as np
from faker import Faker
from kafka import KafkaProducer
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

# Initialize Faker
fake = Faker()
Faker.seed(0)
random.seed(0)
np.random.seed(0)

# Database Setup
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

# Logger Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("producer.log")],
)
log = logging.getLogger("kafka_producer")

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=msgpack.packb,
    retries=5
)

# Fetch Consumers and Devices
def fetch_consumer_devices():
    """Fetch consumer and associated device data from the database"""
    with Session() as session:
        result = session.execute("""
            SELECT c.account_number, d.device_id
            FROM consumers c
            JOIN devices d ON c.account_number = d.account_number
        """)
        data = result.fetchall()

    consumers = {}
    for account_number, device_id in data:
        if account_number not in consumers:
            consumers[account_number] = []
        consumers[account_number].append(device_id)  # Store device_id properly

    return consumers

consumers = fetch_consumer_devices()
consumer_ids = list(consumers.keys())

# Callback Functions for Kafka
def on_send_success(record_metadata):
    """Log successful message delivery"""
    log.info(
        f"Message delivered to {record_metadata.topic} "
        f"[partition {record_metadata.partition}, offset {record_metadata.offset}]"
    )

def on_send_error(excp):
    """Log failed message delivery with error details"""
    log.error(f"Message delivery failed: {excp}", exc_info=True)

# Generate Transactions
def generate_transaction():
    """Generate a transaction with realistic variations"""
    sender, receiver = random.sample(consumer_ids, 2)
    device_id = random.choice(consumers[sender])
    transaction_id = str(fake.uuid4())

    transaction = {
        "transaction_id": transaction_id,
        "timestamp": time.time(),
        "amount": round(np.random.lognormal(mean=5, sigma=1.5), 2),
        "sender": sender,
        "receiver": receiver,
        "device_id": str(device_id),
        "source": random.choice(["INTERNET", "TELEAPP"])
    }
    return transaction_id, transaction

# Produce Transactions to Kafka
def transaction_producer():
    topic = "transactions"
    try:
        while True:
            transaction_id, transaction = generate_transaction()
            producer.send(
                topic,
                key=transaction_id.encode("utf-8"),
                value=transaction
            ).add_callback(on_send_success).add_errback(on_send_error)
            log.debug(f"Queued transaction: {transaction}")
            time.sleep(random.uniform(0.1, 0.5))  # Simulate real transaction intervals
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()  # Ensure all messages are sent
        producer.close()
        log.info("Producer closed.")

if __name__ == "__main__":
    transaction_producer()
