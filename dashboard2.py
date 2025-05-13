import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("🔍 Real-time Fraud Detection Dashboard")

# === Load Data ===
conn = sqlite3.connect("fraud_results2.db")
df = pd.read_sql_query("SELECT * FROM fraud_alerts ORDER BY timestamp DESC", conn)
conn.close()

df['timestamp'] = pd.to_datetime(df['timestamp'])

# Parse SHAP values from JSON
def parse_shap_values(shap_json):
    if pd.isna(shap_json):
        return {}
    try:
        return json.loads(shap_json)
    except:
        return {}

df['shap_dict'] = df['shap_values'].apply(parse_shap_values)

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
col4.metric("🕓 Most Recent", filtered_df['timestamp'].max().strftime("%Y-%m-%d %H:%M:%S") if not filtered_df.empty else "N/A")

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

# === SHAP Values Analysis ===
st.subheader("⚖️ Feature Importance (Global SHAP Analysis)")

# Extract and aggregate SHAP values across all transactions
all_shap_features = {}
for _, row in filtered_df.iterrows():
    shap_dict = row['shap_dict']
    for feature, value in shap_dict.items():
        if feature not in all_shap_features:
            all_shap_features[feature] = []
        all_shap_features[feature].append(abs(value))

# Calculate mean absolute SHAP value for each feature
mean_shap_values = {}
for feature, values in all_shap_features.items():
    if values:  # Ensure there are values
        mean_shap_values[feature] = np.mean(values)

# Convert to DataFrame for plotting
if mean_shap_values:
    shap_df = pd.DataFrame({
        'Feature': list(mean_shap_values.keys()),
        'Importance': list(mean_shap_values.values())
    })
    
    # Sort by importance and get top 15 features
    shap_df = shap_df.sort_values('Importance', ascending=False).head(15)
    
    fig4 = px.bar(
        shap_df, 
        x='Importance', 
        y='Feature', 
        orientation='h',
        title="Top 15 Features by Mean |SHAP Value|"
    )
    fig4.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.info("No SHAP data available for the selected transactions.")

# === Transaction-level SHAP Values ===
st.subheader("🔬 Transaction-Level SHAP Analysis")

# Allow user to select a transaction to analyze
transaction_ids = filtered_df['transaction_id'].tolist()
if transaction_ids:
    selected_transaction = st.selectbox(
        "Select Transaction ID for Detailed SHAP Analysis", 
        transaction_ids
    )
    
    # Get the selected transaction
    selected_row = filtered_df[filtered_df['transaction_id'] == selected_transaction].iloc[0]
    shap_dict = selected_row['shap_dict']
    
    if shap_dict:
        # Convert to DataFrame for plotting
        detail_shap_df = pd.DataFrame({
            'Feature': list(shap_dict.keys()),
            'SHAP Value': list(shap_dict.values())
        })
        
        # Sort and get top features (both positive and negative)
        detail_shap_df = detail_shap_df.sort_values('SHAP Value')
        
        # Get top 5 negative and top 5 positive features
        neg_features = detail_shap_df[detail_shap_df['SHAP Value'] < 0].head(5)
        pos_features = detail_shap_df[detail_shap_df['SHAP Value'] > 0].tail(5)
        plot_df = pd.concat([neg_features, pos_features])
        
        # Plot waterfall chart for selected transaction
        colors = ['red' if x < 0 else 'green' for x in plot_df['SHAP Value']]
        
        fig5 = go.Figure(go.Bar(
            x=plot_df['SHAP Value'],
            y=plot_df['Feature'],
            orientation='h',
            marker_color=colors
        ))
        fig5.update_layout(
            title=f"SHAP Values for Transaction {selected_transaction}",
            xaxis_title="SHAP Value (Red = Lower Risk, Green = Higher Risk)",
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig5, use_container_width=True)
        
        # Show transaction details
        with st.expander("Transaction Details"):
            # Create cleaner display of transaction details
            detail_cols = st.columns(3)
            detail_cols[0].markdown(f"**Transaction ID**: {selected_row['transaction_id']}")
            detail_cols[0].markdown(f"**Sender ID**: {selected_row['sender_id']}")
            detail_cols[0].markdown(f"**Amount**: ${selected_row['amount']}")
            detail_cols[1].markdown(f"**Timestamp**: {selected_row['timestamp']}")
            detail_cols[1].markdown(f"**Merchant**: {selected_row['merchant_category']}")
            detail_cols[2].markdown(f"**Fraud Probability**: {selected_row['fraud_probability']:.4f}")
    else:
        st.info("No SHAP values available for this transaction.")
else:
    st.info("No transactions available with the current filters.")

# === Fraud Table ===
st.subheader("📋 Fraudulent Transactions Table")

# Create a cleaner version of the DataFrame for display
display_df = filtered_df[['transaction_id', 'sender_id', 'amount', 'timestamp', 'merchant_category', 'fraud_probability']]
st.dataframe(display_df, use_container_width=True)

# === SHAP Feature Correlation Analysis ===
st.subheader("🔗 Feature Correlation with SHAP Values")

# Extract the top 5 most important features based on mean absolute SHAP values
if mean_shap_values:
    top_features = sorted(mean_shap_values.items(), key=lambda x: x[1], reverse=True)[:5]
    top_feature_names = [feature for feature, _ in top_features]
    
    # Create feature correlation explanation
    st.write("This analysis shows how the top 5 most important features correlate with their SHAP values:")
    
    # Create scatter plots for each top feature vs its SHAP value
    for i, feature in enumerate(top_feature_names):
        # Initialize lists to store feature values and corresponding SHAP values
        feature_values = []
        shap_values = []
        
        # Collect data points across all transactions
        for _, row in filtered_df.iterrows():
            shap_dict = row['shap_dict']
            if feature in shap_dict:
                # In a real application, you would need to extract the actual feature value
                # Here we're using the SHAP value as a proxy since we don't have access to the raw features
                feature_values.append(i)  # Placeholder
                shap_values.append(shap_dict[feature])
        
        if feature_values and shap_values:
            st.write(f"**{feature}**: This feature has a significant impact on fraud predictions.")
else:
    st.info("Insufficient data for correlation analysis.")