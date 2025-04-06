import random

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
