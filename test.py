import pandas as pd
import joblib
from sklearn.metrics import precision_score, recall_score, f1_score

# Load model and metadata
model = joblib.load("xgb_final2.pkl")
expected_columns = joblib.load("feature_columns.pkl")

categorical_features = [
    "source", "device_os", "browser", "merchant_category",
    "is_international", "country_code", "merchant_risk_level",
    "device_match", "hour_of_day", "day_of_week", "is_weekend", "month"
]

# Load dataset
df_test = pd.read_csv("data.csv")

# Grab 50 frauds and 50 legits
# df_fraud = df[df["fraud_bool"] == 1].head(50)
# df_legit = df[df["fraud_bool"] == 0].head(50)
# df_test = pd.concat([df_fraud, df_legit]).sample(frac=1).reset_index(drop=True)

# Actual labels
y_true = df_test["fraud_bool"].values

# Drop unnecessary columns
drop_cols = [
    'fraud_bool', 'pattern', 'transaction_id', 'sender_id', 'receiver_id',
    'timestamp', 'zip_code', 'ip_address', 'session_id',
    'device_fingerprint', 'transaction_date'
]
df_features = df_test.drop(columns=drop_cols, errors='ignore')

# One-hot encoding
df_encoded = pd.get_dummies(df_features, columns=categorical_features, prefix=categorical_features)

# Align with expected columns
for col in expected_columns:
    if col not in df_encoded:
        df_encoded[col] = 0
df_encoded = df_encoded[expected_columns]

# Predict
fraud_probs = model.predict_proba(df_encoded)[:, 1]  # index 1 = fraud class
THRESHOLD = 0.19
y_pred = (fraud_probs > THRESHOLD).astype(int)

# Evaluation
precision = precision_score(y_true, y_pred)
recall = recall_score(y_true, y_pred)
f1 = f1_score(y_true, y_pred)

# Check model type
print(type(model))
print(f"\n🔍 Evaluation on balanced 100 samples (50 frauds, 50 legits):")
print(f"🟩 Precision: {precision:.4f}")
print(f"🟨 Recall:    {recall:.4f}")
print(f"🟦 F1 Score:  {f1:.4f}")
