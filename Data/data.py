#Imports 
import pandas as pd
import numpy as np
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import hashlib
import yaml
from helper import introduce_ambiguity, weighted_user_sample, calculate_velocity, get_behavioral_features
from fraud_patterns import simulate_burst, simulate_money_laundering, simulate_account_takeover

# Configuration
fake = Faker()
n_records = 1_000_000
fraud_ratio = 0.005
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
date_range = (end_date - start_date).days
records = []
with open("constants.yaml", "r") as file:
    data = yaml.safe_load(file)
merchant_categories = data["merchant_categories"]
merchant_risk = data["merchant_risk"]
browsers = data["browsers"]
browser_weights = data["browser_weights"]
countries = data["countries"]
high_risk_hours = list(range(1, 5))
high_risk_dates = [1, 15, 30]

# User pool generation
user_pool = []
# Highly active users, moderate users, and low activity users
active_users = [str(uuid.uuid4()) for _ in range(500)]
moderate_users = [str(uuid.uuid4()) for _ in range(1500)]
low_activity_users = [str(uuid.uuid4()) for _ in range(2000)]
user_pool.extend(active_users + moderate_users + low_activity_users)

# Create some mule accounts for money laundering patterns
mule_accounts = set(random.sample(user_pool, 30))

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


# Initialize user metadata
for user in user_pool:
    user_balance[user] = max(100, np.random.lognormal(mean=6.5, sigma=1.5))  # Starting balance
    user_avg_txn[user] = max(5, np.random.lognormal(mean=3.5, sigma=1.0))     # Average transaction
    account_age[user] = random.randint(30, 1500)  # Account age in days
    
    # New accounts are higher risk - Reduced probability
    if account_age[user] < 90 and random.random() < 0.0025:
        mule_accounts.add(user)
initial_user_avg_txn = user_avg_txn.copy()

print("Generating synthetic transaction data...")

def main():
    for i in tqdm(range(n_records), desc="Generating records"):
        if random.random() < 0.00001:
            start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_money_laundering(start_time, active_users, moderate_users, low_activity_users, mule_accounts, fake, merchant_categories, sender_zip_history, sender_ip_history, browsers, browser_weights, account_age))
            continue
        
        if random.random() < 0.00003:
            start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_account_takeover(start_time, user_pool, mule_accounts, fake, user_balance, browsers, browser_weights, account_age))
            continue
        
        if random.random() < 0.0003: 
            start_burst_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_burst(start_burst_time, active_users, moderate_users, low_activity_users, fake, merchant_categories, merchant_risk, sender_zip_history, sender_ip_history, mule_accounts, user_balance, user_avg_txn, browsers, browser_weights, account_age, is_fraud=True))
            continue
        
        if random.random() < 0.0001:
            start_burst_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
            records.extend(simulate_burst(start_burst_time, active_users, moderate_users, low_activity_users, fake, merchant_categories, merchant_risk, sender_zip_history, sender_ip_history, mule_accounts, user_balance, user_avg_txn, browsers, browser_weights, account_age, is_fraud=False))
            continue

        # Regular transactions
        is_fraud = np.random.rand() < fraud_ratio
        pattern = "normal"
        
        days_offset = np.random.randint(0, date_range)
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
        
        # Time-based fraud patterns
        if timestamp.hour in high_risk_hours and random.random() < 0.004:
            is_fraud = True
            pattern = "late_night"
        
        if timestamp.day in high_risk_dates and random.random() < 0.002:
            is_fraud = True
            pattern = "payday_fraud"
        
        if timestamp.weekday() in [5, 6] and random.random() < 0.0006:
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
        sender_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
        receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
        
        # First-time user pattern - Reduced probability
        if sender_id not in user_last_txn_time and random.random() < 0.005:  # Reduced from 0.03 to 0.015
            is_fraud = True
            pattern = "new_account_fraud"
        
        # Ensure we're not sending to ourselves
        while receiver_id == sender_id:
            receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
        
        session_id = str(uuid.uuid4())
        
        # Location data
        if sender_id not in sender_zip_history:
            sender_zip_history[sender_id] = fake.zipcode()
        if sender_id not in sender_ip_history:
            sender_ip_history[sender_id] = fake.ipv4_public()
        
        # New location patterns for fraud - Reduced probability
        if is_fraud and random.random() < 0.008:
            # New ZIP code for fraud (different geographical area)
            zip_code = fake.zipcode()
            while zip_code[:1] == sender_zip_history[sender_id][:1]:
                zip_code = fake.zipcode()
            pattern = "location_change"
        else:
            zip_code = sender_zip_history[sender_id]
        
        # IP address patterns
        if is_fraud and random.random() < 0.008: 
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
        
        # High-risk merchant pattern 
        if merchant_category in fraud_merchants and random.random() < 0.012:
            is_fraud = True
            pattern = "high_risk_merchant"
        
        # Specific merchant fraud patterns 
        if merchant_category in ["Gift Cards", "Money Transfer", "Gambling"] and random.random() < 0.008:  
            is_fraud = True
            if pattern == "normal":
                pattern = "suspicious_merchant"
        
        # Mule account pattern
        if receiver_id in mule_accounts and random.random() < 0.025: 
            is_fraud = True
            pattern = "mule_transfer"
        
        # Calculate velocity features
        velocity = calculate_velocity(sender_id, amount, timestamp, user_last_txn_time)
        
        # Calculate behavioral features
        behavior = get_behavioral_features(sender_id, amount, merchant_category, device_os, merchant_risk, user_avg_txn, user_device_history)
        
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
        if random.random() < 0.05:  
            country_code = random.choice(list(countries.keys()))
            if country_code != "US":
                is_international = True
                if random.random() < 0.008: 
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
    df.to_csv("Data/data.csv", index=False)
    print("data.csv")
if __name__ == "__main__":
    print("Synthetic transaction data generation complete.")
    print("Data saved to data.csv.")
    main()