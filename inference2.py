import time
import pandas as pd
import sqlite3
from kafka import KafkaConsumer
import msgpack
import joblib
import smtplib
from email.message import EmailMessage
import shap  # Added SHAP import
import json  # For serializing SHAP values

# === Config ===
BATCH_SIZE = 1
BATCH_TIMEOUT = 2  # seconds
KAFKA_TOPIC = "transactions"
BOOTSTRAP_SERVERS = "localhost:9092"
THRESHOLD = 0.2

# === Load model and training-time columns ===
model = joblib.load("xgb_final.pkl")
expected_columns = joblib.load("feature_columns.pkl")

# === Features used during encoding ===
categorical_features = [
    "source", "device_os", "browser", "merchant_category",
    "is_international", "country_code", "merchant_risk_level",
    "device_match", "hour_of_day", "day_of_week", "is_weekend", "month"
]

# === Setup SQLite ===
conn = sqlite3.connect("fraud_results2.db")
cursor = conn.cursor()

# Modified schema to include shap_values column
cursor.execute("""
CREATE TABLE IF NOT EXISTS fraud_alerts (
    transaction_id TEXT PRIMARY KEY,
    sender_id TEXT,
    amount REAL,
    timestamp TEXT,
    merchant_category TEXT,
    fraud_probability REAL,
    shap_values TEXT
)
""")
conn.commit()

# === Kafka Consumer ===
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
    auto_offset_reset="latest",
    group_id="fraud-detector-batch"
)

def send_fraud_alert_email(user_id, transaction_id, amount, to_email='nikhilarora13832@gmail.com'):
    msg = EmailMessage()
    msg['Subject'] = f'⚠️ Fraudulent Transaction Alert for User {user_id}'
    msg['From'] = 'nikhilarora1729@gmail.com'
    msg['To'] = to_email

    msg.set_content(f'''
    Dear User {user_id},

    A potentially fraudulent transaction was detected:
    - Transaction ID: {transaction_id}
    - Amount: ${amount}

    Please review this transaction immediately.

    Regards,
    Fraud Detection System
    ''')

    # Use Gmail SMTP with App Password
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login('nikhilarora1729@gmail.com', 'tndq nrlh mcwe ebne')
        smtp.send_message(msg)

# === Batch Processor ===
def process_batch(batch):
    df = pd.DataFrame(batch)
    if df.empty:
        return

    # Store original transaction data for reference
    original_transactions = {}
    for tx in batch:
        original_transactions[tx["transaction_id"]] = tx

    drop_cols = [
        'fraud_bool', 'pattern', 'transaction_id', 'sender_id', 'receiver_id',
        'timestamp', 'zip_code', 'ip_address', 'session_id',
        'device_fingerprint', 'transaction_date'
    ]
    df = df.drop(columns=drop_cols, errors='ignore')

    # One-hot encode categorical features
    df_encoded = pd.get_dummies(df, columns=categorical_features, prefix=categorical_features)

    # Align to training column structure
    for col in expected_columns:
        if col not in df_encoded:
            df_encoded[col] = 0
    df_encoded = df_encoded[expected_columns]

    # Predict fraud probabilities
    fraud_probs = model.predict_proba(df_encoded)[:, 0]
    predictions = (fraud_probs > THRESHOLD).astype(int)

    # Calculate SHAP values for the batch
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(df_encoded)
    
    # If model is using 2-class output (fraud/non-fraud), get the fraud class SHAP values
    if isinstance(shap_values, list):
        # For two-class problems, shap_values is often a list with values for each class
        # We want the fraud class (typically index 0 for "probability of fraud")
        shap_values = shap_values[0]

    for i, (tx, prob, pred) in enumerate(zip(batch, fraud_probs, predictions)):
        tx_id = tx["transaction_id"]
        sender = tx["sender_id"]

        if pred == 1:
            print(f"🚨 Fraud Detected: {tx_id} | Prob: {prob:.2f} | Sender: {sender}")
            send_fraud_alert_email(sender, tx_id, tx["amount"])
            
            # Format SHAP values as a dict of feature:shap_value pairs
            # This makes it easier to interpret and query later
            shap_dict = {}
            for j, col in enumerate(expected_columns):
                shap_dict[col] = float(shap_values[i, j])
            
            # Sort by absolute SHAP value to find most important features
            sorted_shap = dict(sorted(shap_dict.items(), key=lambda x: abs(x[1]), reverse=True))
            
            # Convert to JSON for storage
            shap_json = json.dumps(sorted_shap)
            
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO fraud_alerts 
                    (transaction_id, sender_id, amount, timestamp, merchant_category, fraud_probability, shap_values)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tx_id,
                    sender,
                    tx["amount"],
                    tx["timestamp"],
                    tx.get("merchant_category", "unknown"),
                    float(prob),
                    shap_json
                ))
                conn.commit()
                print(f"Stored SHAP values for transaction {tx_id}")
            except Exception as e:
                print(f"DB Insert Error: {e}")
        else:
            print(f"✅ Legit Transaction: {tx_id} | Prob: {prob:.2f}")

# === Inference Loop ===
batch = []
first_ts = None

try:
    for message in consumer:
        tx = message.value

        if not first_ts:
            first_ts = time.time()

        batch.append(tx)

        if len(batch) >= BATCH_SIZE or (time.time() - first_ts) >= BATCH_TIMEOUT:
            process_batch(batch)
            batch = []
            first_ts = None
except KeyboardInterrupt:
    print("Shutting down...")

consumer.close()
conn.close()