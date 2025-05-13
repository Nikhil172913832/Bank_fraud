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
from helper import introduce_ambiguity, weighted_user_sample
from fraud_patterns import simulate_burst, simulate_money_laundering, simulate_account_takeover

# Configuration
fake = Faker()
n_records = 1_000_000  # Total number of records to generate
fraud_ratio = 0.012  # Overall fraud ratio target
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
date_range = (end_date - start_date).days
records = []

# Load constants
with open("constants.yaml", "r") as file:
    data = yaml.safe_load(file)
merchant_categories = list(data["merchant_category"].keys())
merchant_risk = data["merchant_category"]
browsers = data["browsers"]
browser_weights = data["browser_weights"]
countries = data["countries"]
high_risk_hours = list(range(1, 5))
high_risk_dates = [1, 15, 30]

# Define fraud patterns and their target distribution
fraud_patterns = {
    "normal": 0.0,                # Not fraudulent
    "late_night": 0.103,           # Late night transactions (1-5 AM)
    "payday_fraud": 0.082,         # Fraud around payday (1st, 15th, 30th)
    "weekend_fraud": 0.072,        # Weekend fraud
    "micro_fraud": 0.051,          # Small test amounts
    "location_change": 0.08,      # New zip code
    "ip_change": 0.081,            # New IP address
    "location_and_ip_change": 0.07, # Both location and IP changed
    "high_risk_merchant": 0.10,   # High-risk merchant categories
    "suspicious_merchant": 0.08,  # Specific suspicious merchant categories
    "mule_transfer": 0.07,        # Transfer to known mule account
    "insufficient_funds": 0.05,   # Attempt to transfer more than available
    "new_account_fraud": 0.06,    # New account immediate fraud
    "international": 0.06,        # International transactions
    "money_laundering": 0.02,     # Complex money laundering pattern
    "account_takeover": 0.02,     # Account takeover pattern
    "burst_fraud": 0.001           # Burst of fraudulent transactions
}

# Calculate number of fraud transactions needed per pattern
total_fraud = int(n_records * fraud_ratio)
fraud_counts = {pattern: int(total_fraud * ratio) for pattern, ratio in fraud_patterns.items() if ratio > 0}
# Adjust to ensure we have exactly the right number of fraud transactions
remaining = total_fraud - sum(fraud_counts.values())
if remaining > 0:
    for pattern in sorted(fraud_counts.keys()):
        fraud_counts[pattern] += 1
        remaining -= 1
        if remaining == 0:
            break

print(f"Planning to generate {total_fraud} fraudulent transactions across {len(fraud_counts)} patterns")
for pattern, count in fraud_counts.items():
    print(f"  - {pattern}: {count} transactions")

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
sender_zip_history = {}        # Last known zip code
sender_ip_history = {}         # Last known IP
user_device_history = {}       # Store user's typical devices
user_balance = {}              # Track user account balances
user_avg_txn = {}              # Track average transaction amount per user
user_location_history = {}     # Track geographical data
user_last_txn_time = {}        # Track last transaction times
account_age = {}               # Track account ages

# Initialize user metadata
for user in user_pool:
    user_balance[user] = max(100, np.random.lognormal(mean=6.5, sigma=1.5))  # Starting balance
    user_avg_txn[user] = max(5, np.random.lognormal(mean=3.5, sigma=1.0))     # Average transaction
    account_age[user] = random.randint(30, 1500)  # Account age in days
    
    # New accounts are higher risk
    if account_age[user] < 90 and random.random() < 0.0025:
        mule_accounts.add(user)
initial_user_avg_txn = user_avg_txn.copy()

print("Generating synthetic transaction data...")

def generate_transaction(pattern_type=None, is_fraud=False):
    """
    Generate a single transaction with the specified pattern type and fraud status.
    If pattern_type is None, a normal non-fraudulent transaction is generated.
    """
    # Set default pattern if none specified
    if pattern_type is None:
        pattern_type = "normal"
        is_fraud = False
    
    # Generate timestamp based on pattern
    if pattern_type == "late_night":
        # Hours between 1-4 AM
        days_offset = np.random.randint(0, date_range)
        hours_offset = random.choice(high_risk_hours)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
    elif pattern_type == "payday_fraud":
        # Transactions on 1st, 15th or 30th
        days_offset = random.choice([d for d in range(date_range) if (start_date + timedelta(days=d)).day in high_risk_dates])
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
    elif pattern_type == "weekend_fraud":
        # Weekend transactions (Saturday or Sunday)
        # Find days that are weekends
        weekend_days = [d for d in range(date_range) if (start_date + timedelta(days=d)).weekday() in [5, 6]]
        days_offset = random.choice(weekend_days)
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
    else:
        # Regular timestamp for other patterns
        days_offset = np.random.randint(0, date_range)
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        timestamp = start_date + timedelta(days=int(days_offset), hours=hours_offset, minutes=minutes_offset)
    
    # Amount calculation based on pattern
    if pattern_type == "micro_fraud":
        # Small test amounts for this specific fraud pattern
        amount = round(np.random.uniform(0.5, 10.0), 2)
    elif is_fraud:
        # Amounts for other fraud patterns
        if random.random() < 0.7:
            amount = np.random.lognormal(mean=5.0, sigma=1.0)
            if random.random() < 0.2:
                # Some fraudsters use exact dollar amounts
                amount = float(int(amount))
        else:
            amount = np.random.lognormal(mean=4.0, sigma=1.0)
    else:
        # Normal transaction amounts
        amount = np.random.lognormal(mean=3.5, sigma=1.2)
        # Round to nice numbers occasionally
        if random.random() < 0.3:
            amount = round(amount, 0)
    
    # Transaction source - different weights for fraud vs. legitimate
    if is_fraud:
        source = random.choices(
            ["MOBILE_APP", "WEB", "POS", "PHONE", "ATM"], 
            weights=[0.4, 0.5, 0.05, 0.025, 0.025]
        )[0]
    else:
        source = random.choices(
            ["MOBILE_APP", "WEB", "POS", "PHONE", "ATM"], 
            weights=[0.6, 0.3, 0.05, 0.025, 0.025]
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
    while receiver_id == sender_id:
        receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
    # Ensure sender and receiver are different
    
    # For certain patterns, select specific senders or receivers
    if pattern_type == "new_account_fraud":
        # Prioritize users without transaction history
        potential_new_users = [user for user in user_pool if user not in user_last_txn_time]
        if potential_new_users:
            sender_id = random.choice(potential_new_users)
        else:
            # Fallback to users with very new accounts
            newest_users = sorted([(user, age) for user, age in account_age.items()], key=lambda x: x[1])[:20]
            if newest_users:
                sender_id = random.choice([user for user, _ in newest_users])
    
    if pattern_type == "mule_transfer":
        # Use mule account as receiver
        receiver_id = random.choice(list(mule_accounts))
    
    # Ensure sender and receiver are different
    while receiver_id == sender_id:
        receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
    
    session_id = str(uuid.uuid4())
    
    # Location data - initialize if first transaction
    if sender_id not in sender_zip_history:
        sender_zip_history[sender_id] = fake.zipcode()
    if sender_id not in sender_ip_history:
        sender_ip_history[sender_id] = fake.ipv4_public()
    
    # Handle location/IP changes based on pattern
    if pattern_type == "location_change" or pattern_type == "location_and_ip_change":
        # Create a new ZIP code that's different (first digit different)
        current_zip = sender_zip_history[sender_id]
        new_zip = fake.zipcode()
        while new_zip[:1] == current_zip[:1]:
            new_zip = fake.zipcode()
        zip_code = new_zip
    else:
        zip_code = sender_zip_history[sender_id]
    
    # IP address patterns
    if pattern_type == "ip_change" or pattern_type == "location_and_ip_change":
        # Different IP subnet for fraud
        current_ip = sender_ip_history[sender_id]
        new_ip = fake.ipv4_public()
        while new_ip.split('.')[0] == current_ip.split('.')[0]:
            new_ip = fake.ipv4_public()
        ip_address = new_ip
    else:
        ip_address = sender_ip_history[sender_id]
    
    # Merchant category selection
    if pattern_type == "high_risk_merchant":
        # Select from high-risk merchants
        merchant_category = random.choice(list(fraud_merchants))
    elif pattern_type == "suspicious_merchant":
        # Select from specific suspicious categories
        merchant_category = random.choice(["Gift Cards", "Money Transfer", "Gambling"])
    else:
        if is_fraud:
            # Fraud is more likely in high-risk merchant categories
            merchant_category = random.choices(
                merchant_categories, 
                weights=[merchant_risk[m] for m in merchant_categories]
            )[0]
        else:
            # Legitimate transactions are more likely in low-risk merchant categories
            merchant_category = random.choices(
                merchant_categories, 
                weights=[1/merchant_risk[m] for m in merchant_categories]
            )[0]
    
    # International transaction handling
    is_international = False
    country_code = "US"
    if pattern_type == "international":
        # Ensure it's an international transaction
        country_code = random.choice([code for code in countries.keys() if code != "US"])
        is_international = True
    elif random.random() < 0.03:  # Small chance of international for non-international pattern
        country_code = random.choice(list(countries.keys()))
        if country_code != "US":
            is_international = True
    
    # Create hash features for device fingerprinting
    device_fingerprint = None
    if source in ["MOBILE_APP", "WEB"]:
        # Create a fingerprint from device + IP + browser
        fingerprint_str = f"{device_os}|{ip_address}|{browser if browser else ''}"
        device_fingerprint = hashlib.sha256(fingerprint_str.encode()).hexdigest()
    
    # Check if there are sufficient funds (for insufficient_funds pattern)
    has_sufficient_funds = True
    if sender_id in user_balance:
        if pattern_type == "insufficient_funds":
            # Make sure transaction amount exceeds balance
            amount = user_balance[sender_id] * (1 + random.uniform(0.1, 1.0))
            has_sufficient_funds = False
        else:
            has_sufficient_funds = amount <= user_balance[sender_id]
    
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
        "has_sufficient_funds": has_sufficient_funds,
        "pattern": pattern_type,
        "fraud_bool": int(introduce_ambiguity(pattern_type, is_fraud))
    }
    
    # Update user state AFTER creating the transaction
    # Only update for transactions that would be processed (sufficient funds or fraud)
    if has_sufficient_funds or is_fraud:
        # Update location history
        sender_zip_history[sender_id] = zip_code
        sender_ip_history[sender_id] = ip_address
        
        # Update last transaction time
        user_last_txn_time[sender_id] = timestamp
        
        # Update balance (except for insufficient_funds pattern)
        if has_sufficient_funds and sender_id in user_balance:
            user_balance[sender_id] = max(0, user_balance[sender_id] - amount)
    
    return record

def generate_complex_patterns():
    """Generate pre-defined complex fraud patterns"""
    complex_records = []
    
    # Money laundering patterns
    for _ in range(fraud_counts.get("money_laundering", 0)):
        start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
        complex_records.extend(simulate_money_laundering(
            start_time, active_users, moderate_users, low_activity_users, 
            mule_accounts, fake, merchant_categories, sender_zip_history, 
            sender_ip_history, browsers, browser_weights, account_age
        ))
    
    # Account takeover patterns
    for _ in range(fraud_counts.get("account_takeover", 0)):
        start_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
        complex_records.extend(simulate_account_takeover(
            start_time, user_pool, mule_accounts, fake, user_balance,
            browsers, browser_weights, account_age
        ))
    
    # Burst fraud patterns
    for _ in range(fraud_counts.get("burst_fraud", 0)):
        start_burst_time = start_date + timedelta(days=random.randint(0, date_range), seconds=random.randint(0, 86400))
        complex_records.extend(simulate_burst(
            start_burst_time, active_users, moderate_users, low_activity_users,
            fake, merchant_categories, merchant_risk, sender_zip_history, 
            sender_ip_history, mule_accounts, user_balance, user_avg_txn,
            browsers, browser_weights, account_age, is_fraud=True
        ))
    
    return complex_records

def main():
    # First generate complex patterns (these generate multiple records at once)
    complex_records = generate_complex_patterns()
    records.extend(complex_records)
    
    # Calculate how many additional records we need to generate
    complex_count = len(complex_records)
    remaining_count = n_records - complex_count
    
    # Calculate remaining fraud counts
    remaining_fraud_count = sum([count for pattern, count in fraud_counts.items() 
                               if pattern not in ["money_laundering", "account_takeover", "burst_fraud"]])
    remaining_normal_count = remaining_count - remaining_fraud_count
    
    print(f"Generated {complex_count} records from complex patterns")
    print(f"Generating {remaining_fraud_count} fraud records and {remaining_normal_count} normal records")
    
    # Create list of specific fraud records to generate
    fraud_records_to_generate = []
    for pattern, count in fraud_counts.items():
        if pattern not in ["normal", "money_laundering", "account_takeover", "burst_fraud"]:
            fraud_records_to_generate.extend([pattern] * count)
    
    # Shuffle for randomness
    random.shuffle(fraud_records_to_generate)
    
    # Now generate all transactions deterministically
    with tqdm(total=remaining_count, desc="Generating records") as pbar:
        # Generate fraud transactions
        for pattern in fraud_records_to_generate:
            record = generate_transaction(pattern_type=pattern, is_fraud=True)
            records.append(record)
            pbar.update(1)
        
        # Generate remaining normal transactions
        for _ in range(remaining_normal_count):
            record = generate_transaction(pattern_type="normal", is_fraud=False)
            records.append(record)
            pbar.update(1)
    
    # Convert to dataframe
    df = pd.DataFrame(records)
    
    print("\nPost-processing dynamic features...")
    
    # Sort by sender_id and timestamp for accurate time-based calculations
    df = df.sort_values(['sender_id', 'timestamp'])
    
    df['time_since_last_txn'] = df.groupby('sender_id')['timestamp'].diff().dt.total_seconds() / 3600
    df['amount_velocity'] = df['amount'] / df['time_since_last_txn'].replace(0, np.nan)
    df['amount_velocity'] = df['amount_velocity'].fillna(np.nan)
    
    # Calculate device change features
    df['previous_device_os'] = df.groupby('sender_id')['device_os'].shift(1)
    df['previous_device_os'] = df['previous_device_os'].fillna(
    df.groupby('sender_id')['device_os'].transform('first')
)

    df['device_match'] = (df['device_os'] == df['previous_device_os']).astype(int)
    df = df.drop(columns=['previous_device_os'])
    
    def compute_ema_ratio(group):
        ema = group['initial_avg_txn'].iloc[0]
        ratios = []
        for amount in group['amount']:
            ratio = amount / ema if ema != 0 else np.nan  # safer
            ratios.append(ratio)
            ema = 0.9 * ema + 0.1 * amount
        return group.assign(amount_to_average_ratio=ratios)

    df = df.groupby('sender_id', group_keys=False).apply(compute_ema_ratio) # Line 457
    # Check columns AFTER apply - this is likely where 'sender_id' might be lost
    # print("Columns after compute_ema_ratio:", df.columns)
    if 'sender_id' not in df.columns:
         # If sender_id became the index after the apply, reset it
         if 'sender_id' in df.index.names:
              df = df.reset_index()
         else:
              raise ValueError("FATAL: 'sender_id' column lost after compute_ema_ratio apply. Check the apply function or groupby behavior.")


    def compute_velocity(group):
        # This function also seems okay, uses assign
        timestamps = group['timestamp'] # Assumes group is already sorted by timestamp
        velocity_6h = []
        velocity_24h = []
        velocity_4w = []

        for i, now in enumerate(timestamps):
            past_6h = timestamps[(timestamps >= now - pd.Timedelta(hours=6)) & (timestamps < now)]
            past_24h = timestamps[(timestamps >= now - pd.Timedelta(hours=24)) & (timestamps < now)]
            past_4w = timestamps[(timestamps >= now - pd.Timedelta(weeks=4)) & (timestamps < now)]

            velocity_6h.append(len(past_6h))
            velocity_24h.append(len(past_24h))
            velocity_4w.append(len(past_4w))

        return group.assign(velocity_6h=velocity_6h,
                            velocity_24h=velocity_24h,
                            velocity_4w=velocity_4w)

    # --- Second sort where the error occurs ---
    # Ensure sender_id still exists before this sort
    if 'sender_id' not in df.columns:
         raise ValueError(f"FATAL: 'sender_id' column missing before final sort. Columns are: {df.columns}")

    df = df.sort_values(['sender_id', 'timestamp']) # Line 468 (Original error location)
    df = df.groupby('sender_id', group_keys=False).apply(compute_velocity) # Line 469

    # --- Check after compute_velocity as well ---
    if 'sender_id' not in df.columns:
         if 'sender_id' in df.index.names:
              df = df.reset_index()
         else:
              raise ValueError("FATAL: 'sender_id' column lost after compute_velocity apply.")


    # Drop intermediate column
    df = df.drop(columns=['initial_avg_txn'])


    # Clean up None/NaN values selectively
    categorical_cols = ['device_os', 'browser', 'zip_code', 'merchant_category', 'ip_address', 'country_code', 'device_fingerprint', 'pattern']
    numeric_cols_fill_zero = ['velocity_6h', 'velocity_24h', 'velocity_4w'] # Counts can be zero
    numeric_cols_fill_nan = ['amount_velocity', 'time_since_last_txn', 'amount_to_average_ratio'] # Ratios/velocities where NaN is meaningful

    for col in categorical_cols:
        if col in df.columns:
            # Check if the column exists before trying to fill
            df[col] = df[col].fillna("Unknown")
            # Also replace potential None strings if introduced earlier
            df[col] = df[col].replace('None', "Unknown") 

    for col in numeric_cols_fill_zero:
         if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    for col in numeric_cols_fill_nan:
         if col in df.columns:
            # Ensure they are numeric, coercing errors, then fillna is not needed if we want NaN
             df[col] = pd.to_numeric(df[col], errors='coerce')


    # Ensure boolean/int columns don't have lingering NaNs if conversion failed
    bool_int_cols = ['is_international', 'has_sufficient_funds', 'fraud_bool', 'device_match', 'is_weekend']
    for col in bool_int_cols:
        if col in df.columns:
            # Fill potential NaNs introduced by coercion/None with 0 (False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Shuffle records to randomize order
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Add basic time features
    df['hour_of_day'] = df['timestamp'].apply(lambda x: x.hour)
    df['day_of_week'] = df['timestamp'].apply(lambda x: x.weekday())
    df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
    df['month'] = df['timestamp'].apply(lambda x: x.month)
    df['transaction_date'] = df['timestamp'].apply(lambda x: x.date())
    
    # Calculate current fraud percentage
    fraud_count = df['fraud_bool'].sum()
    total_count = len(df)
    fraud_percentage = (fraud_count / total_count) * 100
    
    print(f"\nDataset Summary:")
    print(f"Total Transactions: {total_count}")
    print(f"Fraudulent Transactions: {fraud_count} ({fraud_percentage:.2f}%)")
    
    # Pattern distribution
    print("\nFraud Pattern Distribution:")
    pattern_stats = df.groupby('pattern')['fraud_bool'].agg(['sum', 'count'])
    pattern_stats['fraud_rate'] = pattern_stats['sum'] / pattern_stats['count']
    for pattern, row in pattern_stats.iterrows():
        if row['sum'] > 0:  # Only show patterns with some fraud
            print(f"{pattern:25} {row['sum']:5} / {row['count']:5} = {row['fraud_rate']:.2%}")
    
    # Save the dataset
    df.to_csv("data.csv", index=False)
    print("Data saved to data.csv.")

if __name__ == "__main__":
    main()