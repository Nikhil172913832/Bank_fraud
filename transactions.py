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
import timedelta
from datetime import datetime
import uuid
import hashlib
from collections import defaultdict
start_date = datetime.now()
fake = Faker()
Faker.seed(0)
random.seed(0)
np.random.seed(0)
merchant_categories = [
    "Grocery", "Electronics", "Clothing", "Restaurants", "Gas", "Travel",
    "Health", "Entertainment", "Utilities", "Online Services", "Gambling",
    "Jewelry", "Gift Cards", "Money Transfer"
]

# Risk levels for merchants (higher number = higher risk)
merchant_risk = {
    "Grocery": 1, 
    "Electronics": 4, 
    "Clothing": 2, 
    "Restaurants": 1, 
    "Gas": 2, 
    "Travel": 3, 
    "Health": 2, 
    "Entertainment": 3, 
    "Utilities": 1, 
    "Online Services": 4,
    "Gambling": 5,
    "Jewelry": 5,
    "Gift Cards": 5,
    "Money Transfer": 5
}

# Time-based patterns
high_risk_hours = list(range(1, 5))  # 1 AM to 4 AM
high_risk_dates = [1, 15, 30]  # First, middle, and end of month (payday)

# Browser types for web transactions
browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera", "Unknown"]
browser_weights = [0.45, 0.25, 0.2, 0.05, 0.03, 0.02]
fraud_merchants = set(random.choices(merchant_categories, k=3, weights=[m/sum(merchant_risk.values()) for m in merchant_risk.values()]))
# Database Setup
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
current_time = datetime.now()

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


def fetch_all_consumers():
    with Session() as session:
        result = session.execute("SELECT * FROM consumers")
        columns = result.keys()
        return {
            row[columns.index("account_number")]: dict(zip(columns, row))
            for row in result.fetchall()
        }

consumers = fetch_all_consumers()

mule_accounts = set(random.sample(consumers, 30))
def weighted_user_sample(consumers):
    """Sample users from flat list of dicts based on profile weights"""
    category = random.choices(
        population=["active", "moderate", "low"],
        weights=[0.7, 0.2, 0.1]
    )[0]
    eligible_users = [user for user in consumers if user["user_profile"] == category]
    if not eligible_users:
        return None  # or raise an exception
    return random.choice(eligible_users)["user_id"]
def simulate_burst(start_time, is_fraud=True):
    """Simulate a burst of transactions (typical in fraud scenarios)"""
    burst = []
    sender_id = weighted_user_sample()
    
    # Fraudulent bursts typically come from new locations/devices
    if is_fraud:
        zip_code = fake.zipcode()
        ip_address = fake.ipv4_public()
        if random.random() < 0.5:  # Reduced from 0.7 to 0.5 for risky merchant categories
            selected_merchants = [m for m in merchant_categories if merchant_risk[m] >= 4]
        else:
            selected_merchants = merchant_categories
    else:
        zip_code = random.choice(consumers.get(sender_id).get("zip_history", fake.zipcode()))
        ip_address = random.choice(consumers.get(sender_id).get("ip_history", fake.ipv4_public()))
        selected_merchants = merchant_categories
    #append if not exist
    if zip_code not in consumers[sender_id]["zip_history"]:
        consumers[sender_id]["zip_history"].append(zip_code)
    if ip_address not in consumers[sender_id]["ip_history"]:
        consumers[sender_id]["ip_history"].appendip_address
    # Create the burst transactions
    burst_size = random.randint(3, 15 if is_fraud else 8)
    total_amount = 0
    
    for _ in range(burst_size):
        receiver_id = weighted_user_sample()
        # If fraud, occasionally send to a mule account - Reduced probability
        if is_fraud and random.random() < 0.2:  # Reduced from 0.3 to 0.2
            receiver_id = random.choice(list(mule_accounts))
        # Ensure we're not sending to ourselves
        while receiver_id == sender_id:
            receiver_id = weighted_user_sample()
            
        device_os = random.choices(["Android", "iOS", "Unknown"], weights=[0.6, 0.3, 0.1])[0]
        
        # Amount characteristics for burst transactions
        if is_fraud:
            # Fraudsters try to drain accounts quickly but avoid detection thresholds
            amount = min(
                consumers.get(sender_id).get("balance") * random.uniform(0.1, 0.4),
                random.uniform(50, 500)
            )
            # Sometimes they test with small amounts first
            if random.random() < 0.2:
                amount = round(random.uniform(0.5, 5.0), 2)
        else:
            # Legitimate bursts are usually smaller payments
            amount = round(consumers[sender_id]['avg_txn_amount'] * random.uniform(0.5, 1.5), 2)
        
        total_amount += amount
        consumers[sender_id]['balance'] = max(0, consumers.get(sender_id).get("balance") - amount)
        
        # Merchant pattern
        merchant_category = random.choice(selected_merchants)
        
        # Create transaction entry
        burst.append({
            "transaction_id": str(uuid.uuid4()),
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "timestamp": start_time + timedelta(seconds=random.randint(1, 60)),
            "amount": round(amount, 2),
            "source": "MOBILE_APP" if random.random() < 0.8 else "WEB",
            "device_os": device_os,
            "browser": random.choices(browsers, browser_weights)[0] if device_os == "Unknown" else None,
            "zip_code": zip_code,
            "merchant_category": merchant_category,
            "ip_address": ip_address,
            "session_id": str(uuid.uuid4()),
            "account_age_days": consumers[sender_id]['account_age_days'],
            "fraud_bool": int(introduce_ambiguity("burst", is_fraud)),
            "pattern": "burst_" + ("fraud" if is_fraud else "legitimate")
        })
    
    return burst
def simulate_money_laundering(start_time):
    """Simulate a money laundering pattern with multiple hops"""
    ml_transactions = []
    
    # Initial transaction (usually larger)
    source_sender = weighted_user_sample()
    initial_amount = round(random.uniform(1000, 10000), 2)
    
    # First hop - usually to a mule
    first_mule = random.choice(list(mule_accounts))
    
    # Generate a chain of 3-8 transactions to obscure the money trail
    chain_length = random.randint(3, 8)
    current_amount = initial_amount
    current_sender = source_sender
    current_receiver = first_mule
    
    for i in range(chain_length):
        # Each hop takes a cut (simulates money laundering fees)
        fee_percentage = random.uniform(0.05, 0.15)
        
        if i > 0:
            current_sender = current_receiver
            # After first hop, choose either another mule or regular account
            if random.random() < 0.6:  # Reduced from 0.7 to 0.6
                current_receiver = random.choice(list(mule_accounts - {current_sender}))
            else:
                current_receiver = weighted_user_sample()
                while current_receiver == current_sender or current_receiver in mule_accounts:
                    current_receiver = weighted_user_sample()
        
        # Calculate amount after fee
        hop_amount = current_amount * (1 - fee_percentage) if i > 0 else current_amount
        current_amount = hop_amount
        
        # Add some time delay between hops (minutes to hours)
        hop_time = start_time + timedelta(minutes=random.randint(i*30, i*180))
        
        # Create the transaction
        source = random.choices(["MOBILE_APP", "WEB"], weights=[0.5, 0.5])[0]
        device_os = "Unknown" if source == "WEB" else random.choices(["Android", "iOS"], weights=[0.6, 0.4])[0]
        
        ml_transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "sender_id": current_sender,
            "receiver_id": current_receiver,
            "timestamp": hop_time,
            "amount": round(hop_amount, 2),
            "source": source,
            "device_os": device_os,
            "browser": random.choices(browsers, browser_weights)[0] if source == "WEB" else None,
            "zip_code": random.choice(consumers.get(current_sender).get("zip_history", fake.zipcode())),
            "merchant_category": random.choice(["Money Transfer", "Gift Cards", "Gambling"] 
                                            if random.random() < 0.5 else merchant_categories),
            "ip_address": random.choice(consumers.get(current_sender, {}).get("ip_history", fake.ipv4_public())),
            "session_id": str(uuid.uuid4()),
            "account_age_days": consumers.get(current_sender, {}).get("account_age_days"),
            "fraud_bool": True,
            "is_international": False,
            "pattern": f"money_laundering_hop_{i+1}"
        })

    for transaction in ml_transactions:
        transaction["fraud_bool"] = int(introduce_ambiguity(transaction["pattern"], True))
    return ml_transactions
def simulate_account_takeover(start_time):
    """Simulate an account takeover pattern"""
    takeover_transactions = []
    
    # Select a victim with a decent balance
    victim = random.choice([u for u in consumers if consumers[u]["balance"] > 500])
    
    # Login from new location/device
    new_ip = fake.ipv4_public()
    new_zip = fake.zipcode()
    while new_zip not in consumers[victim]["zip_history"]:
        new_zip = fake.zipcode()
    while new_ip not in consumers[victim]["ip_history"]:
        new_ip = fake.ipv4_public()
    # Password/email change event (not a transaction, but can be tracked)
    account_change = {
        "transaction_id": str(uuid.uuid4()),
        "sender_id": victim,
        "receiver_id": victim,  # self transaction for account changes
        "timestamp": start_time,
        "amount": 0.0,
        "source": random.choices(["WEB", "MOBILE_APP"], weights=[0.8, 0.2])[0],
        "device_os": random.choices(["Windows", "macOS", "Android", "iOS"], weights=[0.4, 0.2, 0.3, 0.1])[0],
        "browser": random.choices(browsers, browser_weights)[0],
        "zip_code": new_zip,
        "merchant_category": "Account Management",
        "ip_address": new_ip,
        "session_id": str(uuid.uuid4()),
        "account_age_days": consumers[victim]['account_age_days'],
        "fraud_bool": True,
        "pattern": "account_takeover_credential_change"
    }
    consumers[victim]['last_transaction_time'] = start_time
    consumers[victim]['zip_history'].append(new_zip)
    consumers[victim]['ip_history'].append(new_ip)
    takeover_transactions.append(account_change)
    
    # Drain the account in 1-3 transactions
    drain_attempts = random.randint(1, 3)
    total_balance = consumers.get(victim)["balance"]
    
    for i in range(drain_attempts):
        # Choose a mule account to receive funds
        mule = random.choice(list(mule_accounts))
        
        # Amount based on remaining balance
        if i == drain_attempts - 1:  # Last transaction takes everything left
            amount = total_balance
        else:
            amount = total_balance * random.uniform(0.4, 0.8)
            total_balance -= amount
        
        # Execute transaction
        drain_tx = {
            "transaction_id": str(uuid.uuid4()),
            "sender_id": victim,
            "receiver_id": mule,
            "timestamp": start_time + timedelta(minutes=random.randint(5, 60)),
            "amount": round(amount, 2),
            "source": account_change["source"],  # Same source as the credential change
            "device_os": account_change["device_os"],  # Same device as the credential change
            "browser": account_change["browser"],
            "zip_code": new_zip,
            "merchant_category": random.choice(["Money Transfer", "Gift Cards"]),
            "ip_address": new_ip,
            "session_id": account_change["session_id"],  # Same session
            "account_age_days": consumers[victim]['account_age_days'],
            "fraud_bool": True,
            "pattern": f"account_takeover_drain_{i+1}"
        }
        takeover_transactions.append(drain_tx)
    
    # Update victim's balance (drained)
    consumers[victim]['balance'] = 0
    for transaction in takeover_transactions:
        transaction["fraud_bool"] = int(introduce_ambiguity(transaction["pattern"], True))
    return takeover_transactions
def introduce_ambiguity(pattern, current_fraud_status):
    """Final ambiguity function with complete pattern coverage"""
    if not current_fraud_status:
        return False  # Only modify actual fraud cases
    
    r = random.random()
    ambiguity_rules = {
        # Account takeover patterns
        'account_takeover': 0.92,       # 8% legitimate
        'credential_change': 0.90,       # 10% legitimate
        'drain': 0.88,                   # 12% legitimate
        
        # Transaction patterns
        'burst': 0.85,                  # 15% legitimate
        'micro': 0.75,                   # 25% legitimate
        'laundering': 0.97,              # 3% legitimate
        'late_night': 0.80,              # 20% legitimate
        
        # Demographic patterns
        'location': 0.70,                # 30% legitimate
        'ip': 0.75,                      # 25% legitimate
        'international': 0.85,           # 15% legitimate
        
        # Merchant patterns
        'high_risk': 0.82,               # 18% legitimate
        'suspicious': 0.88,              # 12% legitimate
        'mule': 0.78,                    # 22% legitimate
        
        # Special cases
        'insufficient': 0.985,           # 1.5% legitimate
        'new_account': 0.70,             # 30% legitimate
        'weekend': 0.80,                 # 20% legitimate
        'payday': 0.75                   # 25% legitimate
    }

    for key, prob in ambiguity_rules.items():
        if key in pattern.lower():
            return r < prob
    
    return current_fraud_status
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
def generate_transaction(sender_id, receiver_id, current_time, fraud_ratio):
    if random.random() < 0.00001:
        start_time = start_date
        return simulate_money_laundering(start_time)  # Should return a list of transactions

    if random.random() < 0.000007:
        start_time = current_time
        return simulate_account_takeover(start_time)

    if random.random() < 0.0003:
        start_burst_time = current_time
        return simulate_burst(start_burst_time, is_fraud=True)

    if random.random() < 0.0002:
        start_burst_time = current_time
        return simulate_burst(start_burst_time, is_fraud=False)

    # === Fraud Decision ===
    is_fraud = np.random.rand() < fraud_ratio
    pattern = "normal"

    if current_time.hour in high_risk_hours and random.random() < 0.004:
        is_fraud = True
        pattern = "late_night"

    if current_time.day in high_risk_dates and random.random() < 0.002:
        is_fraud = True
        pattern = "payday_fraud"

    if current_time.weekday() in [5, 6] and random.random() < 0.0006:
        is_fraud = True
        pattern = "weekend_fraud"

    # === Amount ===
    if is_fraud:
        if random.random() < 0.7:
            amount = np.random.lognormal(mean=5.0, sigma=1.0)
            if random.random() < 0.2:
                amount = float(int(amount))
        else:
            amount = round(np.random.uniform(0.5, 10.0), 2)
            pattern = "micro_fraud"
    else:
        amount = np.random.lognormal(mean=3.5, sigma=1.2)
        if random.random() < 0.3:
            amount = round(amount, 0)

    # === Source, OS, Browser ===
    source = random.choices(
        ["MOBILE_APP", "WEB", "POS", "PHONE", "ATM"],
        weights=[0.6 if not is_fraud else 0.4, 0.3 if not is_fraud else 0.5, 0.05, 0.025, 0.025]
    )[0]
    browser = None
    if source == "MOBILE_APP":
        device_os = random.choices(["Android", "iOS"], weights=[0.7, 0.3])[0]
    elif source == "WEB":
        device_os = random.choices(["Windows", "macOS", "Linux"], weights=[0.6, 0.35, 0.05])[0]
        browser = random.choice(["Chrome", "Firefox", "Safari", "Edge", "Brave"])
    elif source == "POS":
        device_os = "POS_TERMINAL"
    elif source == "PHONE":
        device_os = random.choices(["Android", "iOS", "Unknown"], weights=[0.45, 0.45, 0.1])[0]
    else:
        device_os = "ATM_TERMINAL"

    # === Location/IP ===
    zip_history = consumers[sender_id].get("zip_history")
    zip_code = random.choice(zip_history) if zip_history else fake.zipcode()

    ip_history = consumers[sender_id].get("ip_history")
    ip_address = random.choice(ip_history) if ip_history else fake.ipv4_public()


    if is_fraud and random.random() < 0.008:
        zip_code = fake.zipcode()
        pattern = "location_change"

    if is_fraud and random.random() < 0.008:
        new_ip = fake.ipv4_public()
        while new_ip.split('.')[0] == ip_address.split('.')[0]:
            new_ip = fake.ipv4_public()
        ip_address = new_ip
        pattern = "ip_change" if pattern == "normal" else "location_and_ip_change"

    # === Merchant ===
    merchant_category = random.choices(
        merchant_categories,
        weights=[1 / merchant_risk[m] if not is_fraud else merchant_risk[m] for m in merchant_categories]
    )[0]

    if merchant_category in fraud_merchants and random.random() < 0.012:
        is_fraud = True
        pattern = "high_risk_merchant"

    if merchant_category in ["Gift Cards", "Money Transfer", "Gambling"] and random.random() < 0.008:
        is_fraud = True
        if pattern == "normal":
            pattern = "suspicious_merchant"

    if receiver_id in mule_accounts and random.random() < 0.025:
        is_fraud = True
        pattern = "mule_transfer"

    # === Device fingerprint ===
    device_fingerprint = None
    if source in ["MOBILE_APP", "WEB"]:
        fingerprint_str = f"{device_os}|{ip_address}|{browser or ''}"
        device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()

    # === International ===
    is_international = False
    country_code = "US"
    if random.random() < 0.05:
        country_code = random.choice(["US", "GB", "IN", "DE", "AU", "JP", "CN"])
        if country_code != "US":
            is_international = True
            if random.random() < 0.008:
                is_fraud = True
                pattern = "international"

    # === Velocity & Ratio Features ===
    last_time = consumers[sender_id]("last_txn_time", current_time - timedelta(days=random.randint(1, 30)))
    time_diff = (current_time - last_time).total_seconds() / 60.0
    consumers[sender_id]["last_txn_time"]= current_time

    amount_velocity = round(amount / (time_diff + 1e-6), 2)
    avg_txn = consumers.get(sender_id).get("avg_txn_amount", amount)
    amount_to_avg_ratio = round(amount / (avg_txn + 1e-6), 2)
    device_match = int(device_fingerprint == consumers.get(sender_id, device_fingerprint))

    # === Account balance fraud check ===
    if amount > consumers[sender_id]["balance"] and random.random() < 0.002:
        is_fraud = True
        pattern = "insufficient_funds"
    else:
        consumers[sender_id]["balance"] = max(0, consumers[sender_id]["balance"] - amount)

    return {
        "transaction_id": str(uuid.uuid4()),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "timestamp": current_time,
        "amount": round(amount, 2),
        "source": source,
        "device_os": device_os,
        "browser": browser,
        "zip_code": zip_code,
        "merchant_category": merchant_category,
        "ip_address": ip_address,
        "session_id": str(uuid.uuid4()),
        "account_age_days": consumers[sender_id]['account_age_days'],
        "is_international": is_international,
        "country_code": country_code,
        "device_fingerprint": device_fingerprint,
        "merchant_risk_level": merchant_risk[merchant_category],
        "time_since_last_txn": round(time_diff, 2),
        "amount_velocity": amount_velocity,
        "amount_to_average_ratio": amount_to_avg_ratio,
        "device_match": device_match,
        "fraud_bool": int(is_fraud),
        "pattern": pattern,
        "hour_of_day": current_time.hour,
        "day_of_week": current_time.weekday(),
        "is_weekend": int(current_time.weekday() >= 5),
        "month": current_time.month,
        "transaction_date": current_time.date()
    }
import psycopg2
from psycopg2.extras import execute_batch

def update_consumer_state(updates: list):
    """
    Updates consumer state in the DB.
    Each update dict must have keys:
        user_id, last_transaction_time, balance, zip_history, ip_history,
        avg_txn_amount, device_fingerprint
    """
    conn = psycopg2.connect(
        dbname='bank_fraud',
        user='darklord',
        password='04112005',
        host='localhost',
        port=5432
    )
    cursor = conn.cursor()

    update_query = """
        UPDATE consumers
        SET last_transaction_time = %s,
            balance = %s,
            zip_history = %s,
            ip_history = %s,
            avg_txn_amount = %s,
            device_fingerprint = %s
        WHERE user_id = %s
    """

    data = [
        (
            u["last_transaction_time"],
            u["balance"],
            u["zip_history"],
            u["ip_history"],
            u["avg_txn_amount"],
            u["device_fingerprint"],
            u["user_id"]
        )
        for u in updates
    ]

    execute_batch(cursor, update_query, data)
    conn.commit()
    cursor.close()
    conn.close()
# Produce Transactions to Kafka
def transaction_producer(avg_txn_rate=5):
    topic = "transactions"
    try:
        while True:
            # How many transactions to send in this second?
            num_txns = np.random.poisson(lam=avg_txn_rate)

            for _ in range(num_txns):
                users = random.sample(list(consumers.keys()), 2)
                sender_id = users[0]
                receiver_id = users[1]

                transaction_id, transaction = generate_transaction(
                    sender_id,
                    receiver_id,
                    current_time,
                    fraud_ratio=0.005
                )
                current_time += timedelta(seconds=random.randint(1, 3))
                producer.send(
                    topic,
                    key=transaction_id.encode("utf-8"),
                    value=transaction
                ).add_callback(on_send_success).add_errback(on_send_error)
                log.debug(f"Queued transaction: {transaction}")

            time.sleep(1)  # Wait for the next second
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
        update_consumer_state(consumers.values())
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")

if __name__ == "__main__":
    transaction_producer()
