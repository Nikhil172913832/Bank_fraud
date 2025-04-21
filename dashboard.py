import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("🔍 Real-time Fraud Detection Dashboard")

# === Load Data ===
conn = sqlite3.connect("fraud_results.db")
df = pd.read_sql_query("SELECT * FROM fraud_alerts ORDER BY timestamp DESC", conn)
conn.close()

df['timestamp'] = pd.to_datetime(df['timestamp'])

# === Sidebar Filters ===
with st.sidebar:
    st.header("Filters")
    sender_filter = st.multiselect("Sender ID", df['sender_id'].unique())
    merchant_filter = st.multiselect("Merchant Category", df['merchant_category'].unique())
    prob_threshold = st.slider("Min Fraud Probability", 0.0, 1.0, 0.0, 0.05)

# Apply filters
filtered_df = df.copy()
if sender_filter:
    filtered_df = filtered_df[filtered_df['sender_id'].isin(sender_filter)]
if merchant_filter:
    filtered_df = filtered_df[filtered_df['merchant_category'].isin(merchant_filter)]
filtered_df = filtered_df[filtered_df['fraud_probability'] >= prob_threshold]

# === Summary Cards ===
col1, col2, col3, col4 = st.columns(4)
col1.metric("🚨 Total Frauds", len(filtered_df))
col2.metric("💸 Avg Amount", f"${filtered_df['amount'].mean():.2f}")
col3.metric("🔥 Max Probability", f"{filtered_df['fraud_probability'].max():.2f}")
col4.metric("🕓 Most Recent", filtered_df['timestamp'].max().strftime("%Y-%m-%d %H:%M:%S"))

# === Charts ===
st.subheader("📊 Fraud Probability Distribution")
fig1 = px.histogram(filtered_df, x="fraud_probability", nbins=20, title="Fraud Probability Histogram")
st.plotly_chart(fig1, use_container_width=True)

st.subheader("📈 Fraud Count Over Time")
df_time = filtered_df.copy()
df_time['date'] = df_time['timestamp'].dt.date
time_count = df_time.groupby('date').size().reset_index(name='count')
fig2 = px.line(time_count, x='date', y='count', markers=True)
st.plotly_chart(fig2, use_container_width=True)

st.subheader("🏦 Top Risky Merchants")
top_merchants = filtered_df['merchant_category'].value_counts().nlargest(10).reset_index()
top_merchants.columns = ['merchant_category', 'fraud_count']
fig3 = px.bar(top_merchants, x='merchant_category', y='fraud_count')
st.plotly_chart(fig3, use_container_width=True)

# === Fraud Table ===
st.subheader("📋 Fraudulent Transactions Table")
st.dataframe(filtered_df, use_container_width=True)
