import random
import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from psycopg2.extras import execute_batch
from tqdm import tqdm
import os
# Faker & Random Seed
fake = Faker()
Faker.seed(0)
random.seed(0)
np.random.seed(0)

# Database Connection
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Device probabilities
device_choices = [1, 2, 3, 4]
device_probs = [0.70, 0.20, 0.07, 0.03]
device_oses = ["Windows", "macOS", "Linux", "X11", "other"]

# Batch settings
num_consumers = 1_000_000
batch_size = 10_000

def generate_consumer():
    """Generate consumer and associated devices"""
    account_number = fake.unique.random_number(digits=10, fix_len=True)
    
    num_devices = np.random.choice(device_choices, p=device_probs)
    
    consumer = (
        account_number,
        round(random.uniform(0.1, 0.9), 2),
        random.choice(range(10, 91, 10)),
        fake.random_element(["CA", "CB", "CC", "CD", "CE", "CF", "CG"]),
        fake.random_element(["BA", "BB", "BC", "BD", "BE", "BF", "BG"]),
        round(random.uniform(0, 1), 2),
        fake.boolean(),
        fake.boolean(),
        fake.boolean(),
        random.choice(range(-1, 33)),
        fake.boolean(),
        random.randint(-191, 389),
        random.randint(200, 2000),
    )

    devices = [
        (
            fake.uuid4(),
            account_number,
            random.choice(device_oses),
            random.randint(0, 3),
            random.randint(0, 1),
            fake.boolean()
        )
        for _ in range(num_devices)
    ]
    
    return consumer, devices

# Insert Consumers & Devices in Batches
for _ in tqdm(range(num_consumers // batch_size), desc="Generating Consumers"):
    batch_consumers = []
    batch_devices = []

    for _ in range(batch_size):
        consumer, devices = generate_consumer()
        batch_consumers.append(consumer)
        batch_devices.extend(devices)

    # Insert Consumers
    execute_batch(
        session.connection().connection.cursor(),
        """
        INSERT INTO consumers (
            account_number, income, customer_age, employment_status, housing_status, 
            name_email_similarity, email_is_free, phone_home_valid, phone_mobile_valid, 
            bank_months_count, has_other_cards, credit_risk_score, proposed_credit_limit
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        batch_consumers
    )

    # Insert Devices
    execute_batch(
        session.connection().connection.cursor(),
        """
        INSERT INTO devices (
            device_id, account_number, device_os, 
            device_distinct_emails, device_fraud_count, keep_alive_session
        ) VALUES (%s, %s, %s, %s, %s, %s);
        """,
        batch_devices
    )

    session.commit()

print("Consumer base and devices generation complete!")
