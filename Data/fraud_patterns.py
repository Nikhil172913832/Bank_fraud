from helper import weighted_user_sample, introduce_ambiguity
import random
import uuid
from datetime import timedelta
def simulate_burst(start_time, active_users, moderate_users, low_activity_users, fake, merchant_categories, merchant_risk, sender_zip_history, sender_ip_history, mule_accounts, user_balance, user_avg_txn, browsers, browser_weights, account_age, is_fraud=True):
    """Simulate a burst of transactions (typical in fraud scenarios)"""
    burst = []
    sender_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
    
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
        receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
        # If fraud, occasionally send to a mule account - Reduced probability
        if is_fraud and random.random() < 0.2:  # Reduced from 0.3 to 0.2
            receiver_id = random.choice(list(mule_accounts))
        # Ensure we're not sending to ourselves
        while receiver_id == sender_id:
            receiver_id = weighted_user_sample(active_users, moderate_users, low_activity_users)
            
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

def simulate_money_laundering(start_time, active_users, moderate_users, low_activity_users, mule_accounts, fake, merchant_categories, sender_zip_history, sender_ip_history, browsers, browser_weights, account_age):
    """Simulate a money laundering pattern with multiple hops"""
    ml_transactions = []
    
    # Initial transaction (usually larger)
    source_sender = weighted_user_sample(active_users, moderate_users, low_activity_users)
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
                current_receiver = weighted_user_sample(active_users, moderate_users, low_activity_users)
                while current_receiver == current_sender or current_receiver in mule_accounts:
                    current_receiver = weighted_user_sample(active_users, moderate_users, low_activity_users)
        
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

def simulate_account_takeover(start_time, user_pool, mule_accounts, fake, user_balance, browsers, browser_weights, account_age):
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