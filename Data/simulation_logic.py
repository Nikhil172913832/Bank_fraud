import yaml

from .fraud_patterns import (
    simulate_account_takeover,
    simulate_burst,
    simulate_money_laundering
)

Fraud_Functions = {
    "money_laundering": simulate_money_laundering,
    "account_takeover": simulate_account_takeover,
    "fraudulent_burst": simulate_burst,
    "normal_burst": simulate_burst
}

def load_fraud_pattern_config(yaml_path="fraud_config.yaml"):
    """
    Load the configuration file.
    """
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)
    patterns=[]
    for pattern in config['fraud_patterns']:
        name = pattern['name']
        simulate_fn = Fraud_Functions.get(name)
        if "is_fraud" in pattern:
            is_fraud = pattern['is_fraud']
            simulate_fn = lambda *args, simulate_fn=simulate_fn, is_fraud=is_fraud, **kwargs: simulate_fn(*args, is_fraud=is_fraud, **kwargs)
        
        pattern.append({
            "name": name,
            "probability": pattern['probability'],
            "simulate_fn": simulate_fn,
        })
    return patterns