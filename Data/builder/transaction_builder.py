import random
import uuid
import hashlib
class TransactionBuilder:
    def __init__(self, sender_id, sender_profile, reciever_id, mule_accounts, frequent_receivers, fraud_merchants, fraud_probs, fraud_ratio, merchant_category, sources, time_stamp):
        self.sender_id = sender_id,
        self.user_profile = sender_profile,
        self.reciever_id = reciever_id,
        self.mule_accounts = mule_accounts,
        self.frequent_receivers = frequent_receivers,
        self.fraud_merchants = fraud_merchants,
        self.fraud_probs = fraud_probs, # these are the probabilities for various fraud patterns like money_laundering, account takeover, etc.
        self.fraud_ratio = fraud_ratio, # this is the probability that we will label the transaction as fraud it is randomly it is done as when generating the transaction parameter we can make tweaks in the transaction to make it look as a fraud for example making a remote zip, change in ip, or amount anomalies etc.
        self.base_time = time_stamp,
        self.merchant_category = merchant_category,
        self.sources = sources,
        self.transactions = []
    
    def generate_transactions(self):
        # We are first selecting from this pool only because these complex patterns have dedicated functions whereas other patterns are mere tweaks which can be handled by the parameter generators
        types = ["fraud", "account_takeover", "money_laundering", "fraudulent_burst", "normal_burst", "legit"]
        weights = [self.fraud_ratio, self.fraud_probs["account_takeover"], self.fraud_probs["money_laundering"], self.fraud_probs["fraudulent_burst"], self.fraud_probs["normal_burst"], 1 - self.fraud_ratio - self.fraud_probs["account_takeover"] - self.fraud_probs["money_laundering"] - self.fraud_probs["fraudulent_burst"] - self.fraud_probs["normal_burst"]]
        transaction_type = random.choices(types, weights=weights, k=1)[0]
        if transaction_type == "account_takeover":
            self.transactions.append(self.generate_account_takeover_transaction())
        elif transaction_type == "money_laundering":
            self.transactions.append(self.generate_money_laundering_transaction())
        elif transaction_type == "fraudulent_burst":
            self.transactions.append(self.generate_burst_transaction())
        elif transaction_type == "normal_burst":
            self.transactions.append(self.generate_burst_transaction())
        else:
            raw_context = self.generate_raw_context()
            if self.raw_context_checks(raw_context):
                transaction_type = "fraud"
            raw_transaction = self.generate_raw_transaction(transaction_type == "fraud")




    def generate_raw_context(self):
        return {
            "sender_id": self.sender_id,
            "reciever_id": self.reciever_id,
            "timestamp": self.timestamp,
        }
    def raw_context_checks(self, transaction):
        val = random.random()
        conditions = [
            (lambda t: t.get("timestamp").hour in range(1, 5), self.fraud_probs["high_risk_hours"]),
            (lambda t: t.get("timestamp").day in [1, 15, 30], self.fraud_probs["high_risk_days"]),
            (lambda t: t.get("timestamp").weekday() in [5, 6], self.fraud_probs["high_risk_weekends"]),
            (lambda t: self.user_profile.get("usr_last_txn_time") is None, self.fraud_probs["new_account"]),
            (lambda t: t.get("reciever_id") in self.mule_accounts, self.fraud_probs["mule_accounts"]),
        ]
        for check_fn, threshold in conditions:
            if check_fn(transaction) and val < threshold:
                return True
        return False


