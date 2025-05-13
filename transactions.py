#Imports
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
import uuid
import hashlib
from datetime import datetime, timedelta, date
import psycopg2
from decimal import Decimal
from psycopg2.extras import execute_batch
import yaml

# Initialize logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("producer.log")],
)
log = logging.getLogger("kafka_producer")

# Set seeds for reproducibility
start_date = datetime.now()
fake = Faker()
Faker.seed(0)
random.seed(0)
np.random.seed(0)

#Loading constants from YAML file
with open("constants.yaml", "r") as file:
    data = yaml.safe_load(file)
merchant_categories = data["merchant_categories"]
merchant_risk = data["merchant_risk"]
browsers = data["browsers"]
browser_weights = data["browser_weights"]
countries = data["countries"]
high_risk_hours = list(range(1, 5))
high_risk_dates = [1, 15, 30]

class FraudSimulator:
    def __init__(self):
        # Database Setup
        self.DB_URL = os.getenv("DB_URL")
        assert self.DB_URL, "DB_URL environment variable not set"
        self.engine = create_engine(self.DB_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.current_time = datetime.now()
        

        def custom_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            return obj  # fallback to default for other types
        # Kafka Producer Configuration
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: msgpack.packb(v, default=custom_serializer),
            retries=5
        )
        
        # Fetch consumers and initialize variables
        self.consumers = self.fetch_all_consumers()
        self.fraud_merchants = set(random.choices(
            merchant_categories, 
            k=3, 
            weights=[m/sum(merchant_risk.values()) for m in merchant_risk.values()]
        ))
        self.mule_accounts = set(random.sample(list(self.consumers.keys()), 30))
        
    

    def fetch_all_consumers(self):
        try:
            with psycopg2.connect(self.DB_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM consumers")
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    consumers = {}
                    for row in rows:
                        consumer_dict = dict(zip(columns, row))
                        consumers[consumer_dict["user_id"]] = consumer_dict
                    return consumers
        except Exception as e:
            log.error(f"Error fetching consumers: {e}")
            return {}




    def weighted_user_sample(self):
        """Sample users based on profile weights"""
        category = random.choices(
            population=["active", "moderate", "low"],
            weights=[0.7, 0.2, 0.1]
        )[0]
        
        eligible_users = [
            user_id for user_id, user in self.consumers.items() 
            if user.get("user_profile") == category
        ]
        
        if not eligible_users:
            return random.choice(list(self.consumers.keys()))
            
        return random.choice(eligible_users)

    def introduce_ambiguity(self, pattern, current_fraud_status):
        """Add ambiguity to fraud classification"""
        if not current_fraud_status:
            return False  # Only modify actual fraud cases
        
        r = random.random()
        ambiguity_rules = {
            # Account takeover patterns
            'account_takeover': 0.92,     # 8% legitimate
            'credential_change': 0.90,     # 10% legitimate
            'drain': 0.88,                 # 12% legitimate
            
            # Transaction patterns
            'burst': 0.85,                # 15% legitimate
            'micro': 0.75,                 # 25% legitimate
            'laundering': 0.97,            # 3% legitimate
            'late_night': 0.80,            # 20% legitimate
            
            # Demographic patterns
            'location': 0.70,              # 30% legitimate
            'ip': 0.75,                    # 25% legitimate
            'international': 0.85,         # 15% legitimate
            
            # Merchant patterns
            'high_risk': 0.82,             # 18% legitimate
            'suspicious': 0.88,            # 12% legitimate
            'mule': 0.78,                  # 22% legitimate
            
            # Special cases
            'insufficient': 0.985,         # 1.5% legitimate
            'new_account': 0.70,           # 30% legitimate
            'weekend': 0.80,               # 20% legitimate
            'payday': 0.75                 # 25% legitimate
        }

        for key, prob in ambiguity_rules.items():
            if key in pattern.lower():
                return r < prob
        
        return current_fraud_status

    def simulate_burst(self, start_time, is_fraud=True):
        """Simulate a burst of transactions (typical in fraud scenarios)"""
        burst = []
        sender_id = self.weighted_user_sample()
        
        # Create sender if not exists
        if sender_id not in self.consumers:
            log.warning(f"Sender {sender_id} not found in consumers")
            return burst
            
        # Fraudulent bursts typically come from new locations/devices
        if is_fraud:
            zip_code = fake.zipcode()
            ip_address = fake.ipv4_public()
            is_international = random.random() < 0.05
            if is_international:
                country_code = random.choice(["US", "GB", "IN", "DE", "AU", "JP", "CN"])
            else:
                country_code = "US"
            if random.random() < 0.5:  # Risky merchant categories
                selected_merchants = [m for m in merchant_categories if merchant_risk[m] >= 4]
            else:
                selected_merchants = merchant_categories
        else:
            # Use existing location/device info
            sender_data = self.consumers.get(sender_id, {})
            zip_history = sender_data.get("zip_history", [fake.zipcode()])
            ip_history = sender_data.get("ip_history", [fake.ipv4_public()])
            is_international = random.random() < 0.00005
            if is_international:
                country_code = random.choice(["US", "GB", "IN", "DE", "AU", "JP", "CN"])
            else:
                country_code = "US"
            zip_code = random.choice(zip_history) if zip_history else fake.zipcode()
            ip_address = random.choice(ip_history) if ip_history else fake.ipv4_public()
            selected_merchants = merchant_categories
            
        # Update consumer location history
        if sender_id in self.consumers:
            if "zip_history" not in self.consumers[sender_id]:
                self.consumers[sender_id]["zip_history"] = []
            if "ip_history" not in self.consumers[sender_id]:
                self.consumers[sender_id]["ip_history"] = []
                
            if zip_code not in self.consumers[sender_id]["zip_history"]:
                self.consumers[sender_id]["zip_history"].append(zip_code)
            if ip_address not in self.consumers[sender_id]["ip_history"]:
                self.consumers[sender_id]["ip_history"].append(ip_address)
        
        # Create the burst transactions
        burst_size = random.randint(3, 15 if is_fraud else 8)
        total_amount = 0
        
        for _ in range(burst_size):
            receiver_id = self.weighted_user_sample()
            # If fraud, occasionally send to a mule account
            if is_fraud and random.random() < 0.2:
                receiver_id = random.choice(list(self.mule_accounts))
                
            # Ensure we're not sending to ourselves
            while receiver_id == sender_id:
                receiver_id = self.weighted_user_sample()
                
            device_os = random.choices(["Android", "iOS", "Unknown"], weights=[0.6, 0.3, 0.1])[0]
            
            # Amount characteristics for burst transactions
            sender_data = self.consumers.get(sender_id, {})
            sender_balance = sender_data.get("balance", 1000)
            avg_txn_amount = sender_data.get("avg_txn_amount", 100)
            if avg_txn_amount is None:
                avg_txn_amount = 100
            
            if is_fraud:
                # Fraudsters try to drain accounts quickly but avoid detection thresholds
                amount = min(
                    float(sender_balance) * random.uniform(0.1, 0.4),
                    random.uniform(50, 500)
                )
                # Sometimes they test with small amounts first
                if random.random() < 0.2:
                    amount = round(random.uniform(0.5, 5.0), 2)
            else:
                # Legitimate bursts are usually smaller payments
                amount = round(float(avg_txn_amount) * random.uniform(0.5, 1.5), 2)
            
            total_amount += amount
            
            # Update balance
            if sender_id in self.consumers:
                self.consumers[sender_id]['balance'] = Decimal(max(0, float(sender_balance) - amount))
            
            # Merchant pattern
            merchant_category = random.choice(selected_merchants)
            #device fingerprint
            device_fingerprint = None
            if device_os != "Unknown":
                fingerprint_str = f"{device_os}|{ip_address}|{random.choices(browsers, browser_weights)[0]}"
                device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
            #time since last transaction
            time_since_last_txn = (start_time - sender_data.get("last_txn_time", start_time)).total_seconds() / 60.0
            #amount velocity
            amount_velocity = round(amount / (time_since_last_txn + 1e-6), 2)
            #amount to average ratio
            amount_to_avg_ratio = round(amount / (float(avg_txn_amount) + 1e-6), 2)
            #device match
            device_match = int(device_fingerprint == sender_data.get("device_fingerprint", None))
            # Update sender's last transaction time
            # merchant risk level
            merchant_risk_level = merchant_risk[merchant_category]
            if sender_id in self.consumers:
                self.consumers[sender_id]['last_txn_time'] = start_time
                self.consumers[sender_id]['avg_txn_amount'] = float(avg_txn_amount)*0.9 + amount*0.1
                self.consumers[sender_id]['device_fingerprint'] = device_fingerprint
            # Create transaction entry
            transaction = {
                "transaction_id": str(uuid.uuid4()),
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "timestamp": start_time + timedelta(seconds=random.randint(1, 60)),
                "amount": round(amount, 2),
                "source": "MOBILE_APP" if random.random() < 0.8 else "WEB",
                "device_os": device_os,
                "browser": random.choices(browsers, browser_weights)[0] if device_os == "Unknown" else None,
                "zip_code": zip_code,
                "amount_velocity": amount_velocity,
                "amount_to_average_ratio": amount_to_avg_ratio,
                "device_match": device_match,
                "is_international": is_international,
                "device_fingerprint": device_fingerprint,
                "country_code": country_code,
                "merchant_risk_level": merchant_risk_level,
                "merchant_category": merchant_category,
                "ip_address": ip_address,
                "session_id": str(uuid.uuid4()),
                "account_age_days": self.consumers.get(sender_id, {}).get('account_age_days', 0),
                "fraud_bool": int(self.introduce_ambiguity("burst", is_fraud)),
                "pattern": "burst_" + ("fraud" if is_fraud else "legitimate"),
                "hour_of_day": start_time.hour,
                "day_of_week": start_time.weekday(),
                "is_weekend": int(start_time.weekday() >= 5),
                "month": start_time.month,
                "transaction_date": start_time.date(),
            }
            burst.append(transaction)
        
        return burst

    def simulate_money_laundering(self, start_time):
        """Simulate a money laundering pattern with multiple hops"""
        ml_transactions = []
        
        # Initial transaction (usually larger)
        source_sender = self.weighted_user_sample()
        initial_amount = round(random.uniform(1000, 10000), 2)
        
        # First hop - usually to a mule
        first_mule = random.choice(list(self.mule_accounts))
        
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
                if random.random() < 0.6:
                    current_receiver = random.choice(list(self.mule_accounts - {current_sender}))
                else:
                    current_receiver = self.weighted_user_sample()
                    while current_receiver == current_sender or current_receiver in self.mule_accounts:
                        current_receiver = self.weighted_user_sample()
            
            # Calculate amount after fee
            hop_amount = current_amount * (1 - fee_percentage) if i > 0 else current_amount
            current_amount = hop_amount
            
            # Add some time delay between hops (minutes to hours)
            hop_time = start_time + timedelta(minutes=random.randint(i*30, i*180))
            
            # Get sender data
            sender_data = self.consumers.get(current_sender, {})
            
            # Create the transaction
            source = random.choices(["MOBILE_APP", "WEB"], weights=[0.5, 0.5])[0]
            device_os = "Unknown" if source == "WEB" else random.choices(
                ["Android", "iOS"], 
                weights=[0.6, 0.4]
            )[0]
            
            # Get sender history or generate defaults
            zip_history = sender_data.get("zip_history", [fake.zipcode()])
            ip_history = sender_data.get("ip_history", [fake.ipv4_public()])
            country_code = "US"
            avg_txn_amount = sender_data.get("avg_txn_amount")
            if avg_txn_amount is None:
                avg_txn_amount = 100
            device_fingerprint = None
            if device_os != "Unknown":
                fingerprint_str = f"{device_os}|{random.choice(ip_history) if ip_history else fake.ipv4_public()}|{random.choices(browsers, browser_weights)[0]}"
                device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
            time_since_last_txn = (hop_time - sender_data.get("last_txn_time", start_time)).total_seconds() / 60.0
            amount_velocity = round(hop_amount / (time_since_last_txn + 1e-6), 2)
            amount_to_avg_ratio = round(hop_amount / (float(avg_txn_amount) + 1e-6), 2)
            device_match = int(device_fingerprint == sender_data.get("device_fingerprint", None))
            merchant_risk_level = merchant_risk.get(random.choice(merchant_categories), 1)
            if current_sender in self.consumers:
                self.consumers[current_sender]['last_txn_time'] = hop_time
                self.consumers[current_sender]['avg_txn_amount'] = float(avg_txn_amount)*0.9 + hop_amount*0.1
                self.consumers[current_sender]['device_fingerprint'] = device_fingerprint
            
            transaction = {
                "transaction_id": str(uuid.uuid4()),
                "sender_id": current_sender,
                "receiver_id": current_receiver,
                "timestamp": hop_time,
                "amount": round(hop_amount, 2),
                "source": source,
                "device_os": device_os,
                "browser": random.choices(browsers, browser_weights)[0] if source == "WEB" else None,
                "zip_code": random.choice(zip_history) if zip_history else fake.zipcode(),
                "merchant_category": random.choice(
                    ["Money Transfer", "Gift Cards", "Gambling"] 
                    if random.random() < 0.5 else merchant_categories
                ),
                "ip_address": random.choice(ip_history) if ip_history else fake.ipv4_public(),
                "session_id": str(uuid.uuid4()),
                "account_age_days": sender_data.get("account_age_days", 0),
                "fraud_bool": 1,
                "is_international": False,
                "pattern": f"money_laundering_hop_{i+1}",
                "device_fingerprint": device_fingerprint,
                "country_code": country_code,
                "merchant_risk_level": merchant_risk_level,
                "time_since_last_txn": round(time_since_last_txn, 2),
                "amount_velocity": amount_velocity,
                "amount_to_average_ratio": amount_to_avg_ratio,
                "device_match": device_match,
                "hour_of_day": hop_time.hour,
                "day_of_week": hop_time.weekday(),
                "is_weekend": int(hop_time.weekday() >= 5),
                "month": hop_time.month,
                "transaction_date": hop_time.date()
            }
            ml_transactions.append(transaction)
        return ml_transactions

    def simulate_account_takeover(self, start_time):
        """Simulate an account takeover pattern"""
        takeover_transactions = []
        
        # Select a victim with a decent balance
        victims = [u for u in self.consumers if self.consumers[u].get("balance", 0) > 500]
        if not victims:
            return []  # No suitable victims
            
        victim = random.choice(victims)
        
        # Get victim data or set defaults
        victim_data = self.consumers.get(victim, {})
        victim_zip_history = victim_data.get("zip_history", [fake.zipcode()])
        victim_ip_history = victim_data.get("ip_history", [fake.ipv4_public()])
        
        # Login from new location/device
        new_ip = fake.ipv4_public()
        new_zip = fake.zipcode()
        
        # Initialize history if needed
        if victim in self.consumers:
            if "zip_history" not in self.consumers[victim]:
                self.consumers[victim]["zip_history"] = [new_zip]
            if "ip_history" not in self.consumers[victim]:
                self.consumers[victim]["ip_history"] = [new_ip]
        merchant_risk_level = 5
        is_international =  random.random() < 0.05
        if is_international:
            country_code = random.choice(["US", "GB", "IN", "DE", "AU", "JP", "CN"])
        else:
            country_code = "US"
        device_fingerprint = None
        avg_txn_amount = victim_data.get("avg_txn_amount", 100)
        if avg_txn_amount is None:
            avg_txn_amount = 100
        if random.random() < 0.5:
            device_os = random.choices(["Android", "iOS"], weights=[0.6, 0.4])[0]
            fingerprint_str = f"{device_os}|{new_ip}|{random.choices(browsers, browser_weights)[0]}"
            device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
        else:
            device_os = random.choices(["Windows", "macOS", "Linux"], weights=[0.6, 0.35, 0.05])[0]
            fingerprint_str = f"{device_os}|{new_ip}|{random.choices(browsers, browser_weights)[0]}"
            device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
        time_since_last_txn = (start_time - victim_data.get("last_txn_time", start_time)).total_seconds() / 60.0
        amount_velocity = round(0 / (time_since_last_txn + 1e-6), 2)
        amount_to_avg_ratio = round(0 / (float(avg_txn_amount) + 1e-6))
        device_match = int(device_fingerprint == victim_data.get("device_fingerprint", None))
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
            "account_age_days": victim_data.get('account_age_days', 0),
            "fraud_bool": True,
            "pattern": "account_takeover_credential_change",
            "device_fingerprint": device_fingerprint,
            "country_code": country_code,
            "merchant_risk_level": merchant_risk_level,
            "time_since_last_txn": round(time_since_last_txn, 2),
            "amount_velocity": amount_velocity,
            "amount_to_average_ratio": amount_to_avg_ratio,
            "device_match": device_match,
            "is_international": is_international,
            "hour_of_day": start_time.hour,
            "day_of_week": start_time.weekday(),
            "is_weekend": int(start_time.weekday() >= 5),
            "month": start_time.month,
            "transaction_date": start_time.date()
        }
        
        if victim in self.consumers:
            self.consumers[victim]['last_transaction_time'] = start_time
            self.consumers[victim]['zip_history'].append(new_zip)
            self.consumers[victim]['ip_history'].append(new_ip)
            
        takeover_transactions.append(account_change)
        
        # Drain the account in 1-3 transactions
        drain_attempts = random.randint(1, 3)
        total_balance = victim_data.get("balance", 1000)
        
        for i in range(drain_attempts):
            # Choose a mule account to receive funds
            mule = random.choice(list(self.mule_accounts))
            
            # Amount based on remaining balance
            if i == drain_attempts - 1:  # Last transaction takes everything left
                amount = total_balance
            else:
                amount = float(total_balance) * random.uniform(0.4, 0.8)
                total_balance -= Decimal(amount)
            start_time += timedelta(minutes=random.randint(5, 60))
            avg_txn_amount = victim_data.get("avg_txn_amount", 100)
            if avg_txn_amount is None:
                avg_txn_amount = 100
            time_since_last_txn = (start_time - victim_data.get("last_txn_time", start_time)).total_seconds() / 60.0
            amount_velocity = round(float(amount) / (float(avg_txn_amount)+ 1e-6))
            amount_to_avg_ratio = round(float(amount) / (float(avg_txn_amount) + 1e-6))
            if victim in self.consumers:
                self.consumers[victim]['last_transaction_time'] = start_time
                self.consumers[victim]['avg_txn_amount'] = float(avg_txn_amount)*0.9 + float(amount)*0.1
                self.consumers[victim]['device_fingerprint'] = device_fingerprint

            # Execute transaction
            drain_tx = {
                "transaction_id": str(uuid.uuid4()),
                "sender_id": victim,
                "receiver_id": mule,
                "timestamp": start_time,
                "amount": round(amount, 2),
                "source": account_change["source"],  # Same source as the credential change
                "device_os": account_change["device_os"],  # Same device as the credential change
                "zip_code": new_zip,
                "merchant_category": random.choice(["Money Transfer", "Gift Cards"]),
                "ip_address": new_ip,
                "session_id": account_change["session_id"],  # Same session
                "account_age_days": victim_data.get('account_age_days', 0),
                "fraud_bool": True,
                "browser": account_change["browser"],  # Same browser as the credential change
                "pattern": f"account_takeover_drain_{i+1}",
                "device_fingerprint": device_fingerprint,
                "country_code": country_code,
                "merchant_risk_level": merchant_risk_level,
                "time_since_last_txn": round(time_since_last_txn, 2),
                "amount_velocity": amount_velocity,
                "amount_to_average_ratio": amount_to_avg_ratio,
                "device_match": device_match,
                "is_international": is_international,
                "hour_of_day": start_time.hour,
                "day_of_week": start_time.weekday(),
                "is_weekend": int(start_time.weekday() >= 5),
                "month": start_time.month,
                "transaction_date": start_time.date()
            }
            takeover_transactions.append(drain_tx)
        
        # Update victim's balance (drained)
        if victim in self.consumers:
            self.consumers[victim]['balance'] = 0
            
        # Apply ambiguity to fraud labels
        for transaction in takeover_transactions:
            transaction["fraud_bool"] = int(self.introduce_ambiguity(transaction["pattern"], True))
        return takeover_transactions

    def generate_transaction(self, sender_id, receiver_id, current_time, fraud_ratio=0.005):
        """Generate a single transaction or a pattern of transactions"""
        # Special case simulations
        if random.random() < 0.001:
            return self.simulate_money_laundering(current_time)
        if random.random() < 0.0007:
            return self.simulate_account_takeover(current_time)
        if random.random() < 0.003:
            return self.simulate_burst(current_time, is_fraud=True)
        if random.random() < 0.002:
            return self.simulate_burst(current_time, is_fraud=False)

        # === Fraud Decision ===
        is_fraud = np.random.rand() < fraud_ratio
        pattern = "normal"

        if current_time.hour in high_risk_hours and random.random() < 0.004:
            is_fraud = True
            pattern = "late_night"

        if current_time.day in high_risk_dates and random.random() < 0.002:
            is_fraud = True
            pattern = "payday_fraud"

        if current_time.weekday() in [5, 6] and random.random() < 0.006:
            is_fraud = True
            pattern = "weekend_fraud"
            
        # Get sender data or set defaults
        sender_data = self.consumers.get(sender_id, {})
        sender_balance = sender_data.get("balance", 1000)
        sender_avg_txn = sender_data.get("avg_txn_amount", 100)
        if sender_avg_txn is None:
            sender_avg_txn = 100
        sender_zip_history = sender_data.get("zip_history", [fake.zipcode()])
        sender_ip_history = sender_data.get("ip_history", [fake.ipv4_public()])
        sender_last_txn_time = sender_data.get("last_txn_time", current_time - timedelta(days=random.randint(1, 30)))
        sender_device_fingerprint = sender_data.get("device_fingerprint", None)

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
            browser = random.choices(browsers, browser_weights)[0]
        elif source == "POS":
            device_os = "POS_TERMINAL"
        elif source == "PHONE":
            device_os = random.choices(["Android", "iOS", "Unknown"], weights=[0.45, 0.45, 0.1])[0]
        else:
            device_os = "ATM_TERMINAL"

        # === Location/IP ===
        zip_code = random.choice(sender_zip_history) if sender_zip_history else fake.zipcode()
        ip_address = random.choice(sender_ip_history) if sender_ip_history else fake.ipv4_public()

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

        if merchant_category in self.fraud_merchants and random.random() < 0.003:
            is_fraud = True
            pattern = "high_risk_merchant"

        if merchant_category in ["Gift Cards", "Money Transfer", "Gambling"] and random.random() < 0.004:
            is_fraud = True
            if pattern == "normal":
                pattern = "suspicious_merchant"

        if receiver_id in self.mule_accounts and random.random() < 0.012:
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
                if random.random() < 0.004:
                    is_fraud = True
                    pattern = "international"

        # === Velocity & Ratio Features ===
        time_diff = (current_time - sender_last_txn_time).total_seconds() / 60.0
        
        # Update last transaction time
        if sender_id in self.consumers:
            self.consumers[sender_id]["last_txn_time"] = current_time

        amount_velocity = round(amount / (time_diff + 1e-6), 2)
        amount_to_avg_ratio = round(amount / (float(sender_avg_txn) + 1e-6), 2)
        device_match = int(device_fingerprint == sender_device_fingerprint)

        # === Account balance fraud check ===
        if amount > sender_balance and random.random() < 0.001:
            is_fraud = True
            pattern = "insufficient_funds"
        else:
            # Update balance
            if sender_id in self.consumers:
                self.consumers[sender_id]["balance"] = max(0, float(sender_balance) - amount)


        # Create transaction
        transaction = {
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
            "account_age_days": sender_data.get('account_age_days', 0),
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
        
        return [transaction]  # Return as list for consistency with other methods

    def update_consumer_state(self):
        """Updates consumer state in the DB."""
        try:
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

            data = []
            for user_id, user_data in self.consumers.items():
                data.append((
                    user_data.get("last_transaction_time"),
                    user_data.get("balance", 0),
                    user_data.get("zip_history", []),
                    user_data.get("ip_history", []),
                    user_data.get("avg_txn_amount", 0),
                    user_data.get("device_fingerprint", None),
                    user_id
                ))

            execute_batch(cursor, update_query, data)
            conn.commit()
            cursor.close()
            conn.close()
            log.info("Consumer state updated successfully")
        except Exception as e:
            log.error(f"Error updating consumer state: {e}")

    def on_send_success(self, record_metadata):
        """Log successful message delivery"""
        log.info(
            f"Message delivered to {record_metadata.topic} "
            f"[partition {record_metadata.partition}, offset {record_metadata.offset}]"
        )

    def on_send_error(self, excp):
        """Log failed message delivery with error details"""
        log.error(f"Message delivery failed: {excp}", exc_info=True)

    def run_transaction_producer(self, avg_txn_rate=5):
        """Main method to produce transactions to Kafka."""
        topic = "transactions"
        try:
            while True:
                num_txns = np.random.poisson(lam=avg_txn_rate)

                for _ in range(num_txns):
                    user_ids = list(self.consumers.keys())
                    if len(user_ids) < 2:
                        log.warning("Not enough users to create transactions")
                        continue

                    sender_id, receiver_id = random.sample(user_ids, 2)

                    transactions = self.generate_transaction(
                        sender_id,
                        receiver_id,
                        self.current_time,
                        fraud_ratio=0.005
                    )

                    for transaction in transactions:
                        transaction_id = transaction["transaction_id"]
                        transaction["timestamp"] = transaction["timestamp"].isoformat()
                        if transaction['fraud_bool'] == 1:
                            print(f"Fraud transaction: {transaction}")
                        self.producer.send(
                            topic,
                            key=transaction_id.encode("utf-8"),
                            value=transaction
                        ).add_callback(self.on_send_success).add_errback(self.on_send_error)
                        log.debug(f"Produced transaction: {transaction_id}")

                self.update_consumer_state()
                self.producer.flush()
                self.current_time = datetime.now()
                time.sleep(1)

        except KeyboardInterrupt:
            log.info("Transaction producer stopped by user.")
        except Exception as e:
            log.error(f"Error in transaction producer: {e}", exc_info=True)
        finally:
            log.info("Closing Kafka producer...")
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)

        
# Example usage:
if __name__ == "__main__":
    simulator = FraudSimulator()
    simulator.run_transaction_producer(avg_txn_rate=5)
