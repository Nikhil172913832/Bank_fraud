import random
def introduce_ambiguity(pattern, current_fraud_status):
    """Final ambiguity function with complete pattern coverage"""
    if not current_fraud_status:
        return False  
    
    r = random.random()
    ambiguity_rules = {
        'account_takeover': 0.92, 
        'credential_change': 0.90,      
        'drain': 0.88,                  
        
        # Transaction patterns
        'burst': 0.85,                  
        'micro': 0.75,                  
        'laundering': 0.97,              
        'late_night': 0.80,              
        
        # Demographic patterns
        'location': 0.70,                
        'ip': 0.75,                     
        'international': 0.85,          
        
        # Merchant patterns
        'high_risk': 0.82,               
        'suspicious': 0.88,              
        'mule': 0.78,                    
        
        # Special cases
        'insufficient': 0.985,           
        'new_account': 0.70,            
        'weekend': 0.80,                 
        'payday': 0.75                  
    }

    for key, prob in ambiguity_rules.items():
        if key in pattern.lower():
            return r < prob
    
    return current_fraud_status

def weighted_user_sample(active_users, moderate_users, low_activity_users):
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

def calculate_velocity(user_id, amount, timestamp, user_last_txn_time):
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
def get_behavioral_features(user_id, amount, merchant, device_os, merchant_risk, user_avg_txn, user_device_history):
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