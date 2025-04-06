import pandas as pd
import numpy as np
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import hashlib

fake = Faker()

# Configuration
n_records = 1_000_000
fraud_ratio = 0.005  # Reduced from 0.008 to 0.005 (0.5% baseline fraud)
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

records = []

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

# Simulate user pool with skewed activity
user_pool = []
active_users = [str(uuid.uuid4()) for _ in range(500)]  # Highly active
moderate_users = [str(uuid.uuid4()) for _ in range(1500)]  # Moderate
low_activity_users = [str(uuid.uuid4()) for _ in range(2000)]  # Low
user_pool.extend(active_users + moderate_users + low_activity_users)

# Create some mule accounts for money laundering patterns
mule_accounts = set(random.sample(user_pool, 30))  # 30 mule accounts

# Known frequent receivers (popular businesses, recurring payments, etc.)
frequent_receivers = random.sample(user_pool, 50)

# Merchant blacklist (historically associated with fraud)
fraud_merchants = set(random.choices(merchant_categories, k=3, weights=[m/sum(merchant_risk.values()) for m in merchant_risk.values()]))

# User metadata storage
sender_zip_history = {}  # Last known zip code
sender_ip_history = {}   # Last known IP
user_device_history = {} # Store user's typical devices
user_balance = {}        # Track user account balances
user_avg_txn = {}        # Track average transaction amount per user
user_location_history = {}  # Track geographical data
user_last_txn_time = {}  # Track last transaction times
account_age = {}         # Track account ages

# Countries and their phone code prefixes for international transactions
countries = {
    "US": "+1", 
    "CA": "+1", 
    "UK": "+44", 
    "AU": "+61", 
    "IN": "+91", 
    "NG": "+234", 
    "RU": "+7", 
    "CN": "+86"
}

# Initialize user metadata
for user in user_pool:
    user_balance[user] = max(100, np.random.lognormal(mean=6.5, sigma=1.5))  # Starting balance
    user_avg_txn[user] = max(5, np.random.lognormal(mean=3.5, sigma=1.0))     # Average transaction
    account_age[user] = random.randint(30, 1500)  # Account age in days
    
    # New accounts are higher risk - Reduced probability
    if account_age[user] < 90 and random.random() < 0.25:  # Reduced from 0.4 to 0.25
        mule_accounts.add(user)
initial_user_avg_txn = user_avg_txn.copy()
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

def weighted_user_sample():
    """Sample users with appropriate weights to simulate real transaction patterns"""
    category = random.choices(
        population=["active", "moderate", "low"],
        weights=[0.7, 0.2, 0.1]
    )[0]
    if category == "active":
        return random.choice(active_users)
    elif category == "moderate":
        return random.choice(moderate_users)
    else:
        return random.choice(low_activity_users)

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
        zip_code = sender_zip_history.get(sender_id, fake.zipcode())
        ip_address = sender_ip_history.get(sender_id, fake.ipv4_public())
        selected_merchants = merchant_categories
        
    sender_zip_history[sender_id] = zip_code
    sender_ip_history[sender_id] = ip_address
    
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
                user_balance.get(sender_id, 1000) * random.uniform(0.1, 0.4),
                random.uniform(50, 500)
            )
            # Sometimes they test with small amounts first
            if random.random() < 0.2:
                amount = round(random.uniform(0.5, 5.0), 2)
        else:
            # Legitimate bursts are usually smaller payments
            amount = round(user_avg_txn.get(sender_id, 20) * random.uniform(0.5, 1.5), 2)
        
        total_amount += amount
        user_balance[sender_id] = max(0, user_balance.get(sender_id, 1000) - amount)
        
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
            "account_age_days": account_age[sender_id],
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
            "zip_code": sender_zip_history.get(current_sender, fake.zipcode()),
            "merchant_category": random.choice(["Money Transfer", "Gift Cards", "Gambling"] 
                                             if random.random() < 0.5 else merchant_categories),  # Reduced from 0.7 to 0.5
            "ip_address": sender_ip_history.get(current_sender, fake.ipv4_public()),
            "session_id": str(uuid.uuid4()),
            "account_age_days": account_age[current_sender],
            "fraud_bool": True,
            "pattern": f"money_laundering_hop_{i+1}"
        })
    for transaction in ml_transactions:
        transaction["fraud_bool"] = int(introduce_ambiguity(transaction["pattern"], True))
    return ml_transactions

def simulate_account_takeover(start_time):
    """Simulate an account takeover pattern"""
    takeover_transactions = []
    
    # Select a victim with a decent balance
    victim = random.choice([u for u in user_pool if user_balance.get(u, 0) > 500])
    
    # Login from new location/device
    new_ip = fake.ipv4_public()
    new_zip = fake.zipcode()
    
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
        "account_age_days": account_age[victim],
        "fraud_bool": True,
        "pattern": "account_takeover_credential_change"
    }
    takeover_transactions.append(account_change)
    
    # Drain the account in 1-3 transactions
    drain_attempts = random.randint(1, 3)
    total_balance = user_balance.get(victim, 1000)
    
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
            "account_age_days": account_age[victim],
            "fraud_bool": True,
            "pattern": f"account_takeover_drain_{i+1}"
        }
        takeover_transactions.append(drain_tx)
    
    # Update victim's balance (drained)
    user_balance[victim] = 0
    for transaction in takeover_transactions:
        transaction["fraud_bool"] = int(introduce_ambiguity(transaction["pattern"], True))
    return takeover_transactions

def calculate_velocity(user_id, amount, timestamp):
    """Calculate transaction velocity features"""
    last_time = user_last_txn_time.get(user_id)
    user_last_txn_time[user_id] = timestamp
    
    if last_time is None:
        return {
            "time_since_last_txn": None,
            "amount_velocity": None
        }
    
    time_diff = (timestamp - last_time).total_seconds() / 3600  # hours
    
    return {
        "time_since_last_txn": time_diff,
        "amount_velocity": amount / max(1, time_diff)  # amount per hour
    }

def get_behavioral_features(user_id, amount, merchant, source, device_os):
    """Generate behavioral biometrics and user profile features"""
    # Compare to user's average transaction
    avg_amount = user_avg_txn.get(user_id, 20)
    amount_ratio = amount / avg_amount if avg_amount > 0 else 1
    
    typical_device = user_device_history.get(user_id, device_os)
    device_match = int(typical_device == device_os)
    user_device_history[user_id] = device_os  # Update history
    
    # Update user average transaction amount
    user_avg_txn[user_id] = 0.9 * avg_amount + 0.1 * amount
    
    return {
        "amount_to_average_ratio": amount_ratio,
        "device_match": device_match,
        "merchant_risk": merchant_risk.get(merchant, 1)
    }

print("Generating synthetic transaction data...")

# Generate transactions throughout the date range
date_range = (end_date - start_date).days
records = []

# Special pattern flags - Further reduced frequencies
include_money_laundering = True
include_account_takeover = True
include_legitimate_bursts = True
def main():
    for i in tqdm(range(n_records), desc="Generating records"):
        # Occasional special fraud patterns - Significantly reduced frequencies
        if random.random() < 0.00001 and include_money_laundering:  # Reduced from 0.0002 to 0.0001
            start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_money_laundering(start_time))
            continue
        
        if random.random() < 0.000007 and include_account_takeover:  # Reduced from 0.00015 to 0.00007
            start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_account_takeover(start_time))
            continue
        
        if random.random() < 0.0003:  # Reduced from 0.0005 to 0.0003 for fraudulent bursts
            start_burst_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_burst(start_burst_time, is_fraud=True))
            continue
        
        if random.random() < 0.0002 and include_legitimate_bursts:  # Keep legitimate bursts the same
            start_burst_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_burst(start_burst_time, is_fraud=False))
            continue

        # Regular transactions
        is_fraud = np.random.rand() < fraud_ratio
        pattern = "normal"
        
        days_offset = np.random.randint(0, date_range)
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
        
        # Time-based fraud patterns - Further reduced probabilities for fraud
        if timestamp.hour in high_risk_hours and random.random() < 0.004:  # Reduced from 0.06 to 0.04
            is_fraud = True
            pattern = "late_night"
        
        if timestamp.day in high_risk_dates and random.random() < 0.002:  # Reduced from 0.03 to 0.02
            is_fraud = True
            pattern = "payday_fraud"
        
        if timestamp.weekday() in [5, 6] and random.random() < 0.0006:  # Reduced from 0.01 to 0.006
            is_fraud = True
            pattern = "weekend_fraud"
        
        # Amount calculation
        if is_fraud:
            # Fraudulent amounts have different patterns
            if random.random() < 0.7:
                # Larger amounts for most fraud
                amount = np.random.lognormal(mean=5.0, sigma=1.0)
                if random.random() < 0.2:
                    # Some fraudsters go for exact dollar amounts (less variable)
                    amount = float(int(amount))
            else:
                # Some fraudsters test with small amounts
                amount = round(np.random.uniform(0.5, 10.0), 2)
                pattern = "micro_fraud"
        else:
            # Normal transaction amounts
            amount = np.random.lognormal(mean=3.5, sigma=1.2)
            # Round to nice numbers occasionally
            if random.random() < 0.3:
                amount = round(amount, 0)
        
        # Transaction source
        source = random.choices(
            ["MOBILE_APP", "WEB", "POS", "PHONE", "ATM"], 
            weights=[0.6 if not is_fraud else 0.4, 
                    0.3 if not is_fraud else 0.5, 
                    0.05, 
                    0.025, 
                    0.025]
        )[0]
        
        # Device OS selection
        device_os = "Unknown"
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
        elif source == "ATM":
            device_os = "ATM_TERMINAL"
        
        # User selection
        sender_id = weighted_user_sample()
        receiver_id = weighted_user_sample()
        
        # First-time user pattern - Reduced probability
        if sender_id not in user_last_txn_time and random.random() < 0.005:  # Reduced from 0.03 to 0.015
            is_fraud = True
            pattern = "new_account_fraud"
        
        # Ensure we're not sending to ourselves
        while receiver_id == sender_id:
            receiver_id = weighted_user_sample()
        
        session_id = str(uuid.uuid4())
        
        # Location data
        if sender_id not in sender_zip_history:
            sender_zip_history[sender_id] = fake.zipcode()
        if sender_id not in sender_ip_history:
            sender_ip_history[sender_id] = fake.ipv4_public()
        
        # New location patterns for fraud - Reduced probability
        if is_fraud and random.random() < 0.008:  # Reduced from 0.12 to 0.08
            # New ZIP code for fraud (different geographical area)
            zip_code = fake.zipcode()
            while zip_code[:1] == sender_zip_history[sender_id][:1]:
                zip_code = fake.zipcode()
            pattern = "location_change"
        else:
            zip_code = sender_zip_history[sender_id]
        
        # IP address patterns - Reduced probability
        if is_fraud and random.random() < 0.008:  # Reduced from 0.12 to 0.08
            # Different IP subnet for fraud
            ip_address = fake.ipv4_public()
            while ip_address.split('.')[0] == sender_ip_history[sender_id].split('.')[0]:
                ip_address = fake.ipv4_public()
            
            if pattern == "location_change":
                pattern = "location_and_ip_change"
            else:
                pattern = "ip_change"
        else:
            ip_address = sender_ip_history[sender_id]
        
        # Merchant category selection
        merchant_category = random.choices(
            merchant_categories, 
            weights=[1/merchant_risk[m] if not is_fraud else merchant_risk[m] for m in merchant_categories]
        )[0]
        
        # High-risk merchant pattern - Reduced probability
        if merchant_category in fraud_merchants and random.random() < 0.012:  # Reduced from 0.2 to 0.12
            is_fraud = True
            pattern = "high_risk_merchant"
        
        # Specific merchant fraud patterns - Reduced probability
        if merchant_category in ["Gift Cards", "Money Transfer", "Gambling"] and random.random() < 0.008:  # Reduced from 0.12 to 0.08
            is_fraud = True
            if pattern == "normal":
                pattern = "suspicious_merchant"
        
        # Mule account pattern - Reduced probability
        if receiver_id in mule_accounts and random.random() < 0.025:  # Reduced from 0.35 to 0.25
            is_fraud = True
            pattern = "mule_transfer"
        
        # Calculate velocity features
        velocity = calculate_velocity(sender_id, amount, timestamp)
        
        # Calculate behavioral features
        behavior = get_behavioral_features(sender_id, amount, merchant_category, source, device_os)
        
        # Update user balance
        if sender_id in user_balance:
            # Check for insufficient funds (potential fraud)
            if amount > user_balance[sender_id] and random.random() < 0.002:  # Reduced from 0.3 to 0.2
                is_fraud = True
                pattern = "insufficient_funds"
            else:
                user_balance[sender_id] = max(0, user_balance[sender_id] - amount)
        
        # Create hash features for device fingerprinting
        device_fingerprint = None
        if source in ["MOBILE_APP", "WEB"]:
            # Create a fingerprint from device + IP + browser
            fingerprint_str = f"{device_os}|{ip_address}|{browser if browser else ''}"
            device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
            # International transaction flag
        is_international = False
        country_code = "US"
        if random.random() < 0.05:  # 5% of transactions are international
            country_code = random.choice(list(countries.keys()))
            if country_code != "US":
                is_international = True
                if random.random() < 0.008:  # International fraud rate reduced from 0.12 to 0.08
                    is_fraud = True
                    pattern = "international"
        
        # Build the transaction record
        record = {
            "transaction_id": str(uuid.uuid4()),
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "timestamp": timestamp,
            "amount": round(amount, 2),
            "source": source,
            "device_os": device_os,
            "browser": browser,
            "zip_code": zip_code,
            "merchant_category": merchant_category,
            "ip_address": ip_address,
            "session_id": session_id,
            "account_age_days": account_age[sender_id],
            "is_international": is_international,
            "country_code": country_code,
            "device_fingerprint": device_fingerprint,
            "merchant_risk_level": merchant_risk[merchant_category],
            "initial_avg_txn": initial_user_avg_txn[sender_id],
            "fraud_bool": int(introduce_ambiguity(pattern, is_fraud)),
            "pattern": pattern
        }
        
        records.append(record)

    # Convert to dataframe 
    df = pd.DataFrame(records)

    print("\nPost-processing dynamic features...")
    df.sort_values(['sender_id', 'timestamp'], inplace=True)
    df['time_since_last_txn'] = df.groupby('sender_id')['timestamp'].diff().dt.total_seconds() / 3600
    df['amount_velocity'] = df['amount'] / df['time_since_last_txn'].replace(0, np.nan)
    df['amount_velocity'].fillna(0, inplace=True)
    df['previous_device_os'] = df.groupby('sender_id')['device_os'].shift(1)
    df['previous_device_os'] = df.groupby('sender_id')['previous_device_os'].fillna(df['device_os'])
    df['device_match'] = (df['device_os'] == df['previous_device_os']).astype(int)
    df.drop(columns=['previous_device_os'], inplace=True)
    def compute_ema_ratio(group):
        ema = group['initial_avg_txn'].iloc[0]
        ratios = []
        for amount in group['amount']:
            ratios.append(amount / ema if ema != 0 else 1.0)
            ema = 0.9 * ema + 0.1 * amount
        group['amount_to_average_ratio'] = ratios
        return group

    df = df.groupby('sender_id').apply(compute_ema_ratio)

    # Calculate current fraud percentage
    fraud_count = df['fraud_bool'].sum()
    total_count = len(df)
    fraud_percentage = (fraud_count / total_count) * 100

    print(f"\nDataset Summary:")
    print(f"Total Transactions: {total_count}")
    print(f"Fraudulent Transactions: {fraud_count} ({fraud_percentage:.2f}%)")

    # Clean up None values
    for column in df.columns:
        df[column] = df[column].apply(lambda x: "Unknown" if x is None else x)

    # Shuffle records to randomize order
    df = df.sample(frac=1).reset_index(drop=True)

    # Add basic feature engineering
    df['hour_of_day'] = df['timestamp'].apply(lambda x: x.hour)
    df['day_of_week'] = df['timestamp'].apply(lambda x: x.weekday())
    df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
    df['month'] = df['timestamp'].apply(lambda x: x.month)
    df['transaction_date'] = df['timestamp'].apply(lambda x: x.date())
    df.drop(columns=['initial_avg_txn'], inplace=True)

    # Pattern distribution
    print("\nAdjusted Fraud Pattern Distribution:")
    pattern_stats = df.groupby('pattern')['fraud_bool'].agg(['sum', 'count'])
    pattern_stats['fraud_rate'] = pattern_stats['sum'] / pattern_stats['count']
    for pattern, row in pattern_stats.iterrows():
        if row['sum'] > 0:  # Only show patterns with some fraud
            print(f"{pattern:25} {row['sum']:5} / {row['count']:5} = {row['fraud_rate']:.2%}")
    df.to_csv("data.csv", index=False)
    print("data.csv")
if __name__ == "__main__":
    print("Synthetic transaction data generation complete.")
    print("Data saved to data.csv.")
    main()