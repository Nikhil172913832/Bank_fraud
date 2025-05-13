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