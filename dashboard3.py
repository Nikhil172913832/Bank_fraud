import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np
from streamlit_extras.metric_cards import style_metric_cards

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("🔍 Real-time Fraud Detection Dashboard")

# Custom CSS for styling
st.markdown("""
<style>
    [data-testid=stSidebar] {
        background: linear-gradient(180deg, #2e2e2e 0%, #1a1a1a 100%);
        color: white;
    }
    .metric-card {
        background: #1a1a1a !important;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stPlotlyChart {
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .css-1q8dd3e {
        color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

# === Load Data ===
@st.cache_data(ttl=300)
def load_data():
    conn = sqlite3.connect("fraud_results2.db")
    df = pd.read_sql_query("SELECT * FROM fraud_alerts ORDER BY timestamp DESC", conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

df = load_data()

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
    st.header("🔧 Filters & Controls")
    sender_filter = st.multiselect("Sender ID", df['sender_id'].unique())
    merchant_filter = st.multiselect("Merchant Category", df['merchant_category'].unique())
    prob_threshold = st.slider("Minimum Fraud Probability", 0.0, 1.0, 0.0, 0.05)
    st.markdown("---")
    st.markdown("**🔍 Quick Analysis:**")
    if st.button("Show Data Summary"):
        st.session_state.show_summary = not st.session_state.get('show_summary', False)

# Apply filters
filtered_df = df.copy()
if sender_filter:
    filtered_df = filtered_df[filtered_df['sender_id'].isin(sender_filter)]
if merchant_filter:
    filtered_df = filtered_df[filtered_df['merchant_category'].isin(merchant_filter)]
filtered_df = filtered_df[filtered_df['fraud_probability'] >= prob_threshold]

# === Summary Cards ===
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="🚨 Total Fraud Alerts", value=len(filtered_df), delta_color="off")
with col2:
    st.metric(label="💸 Average Amount", value=f"${filtered_df['amount'].mean():.2f}", delta_color="off")
with col3:
    st.metric(label="🔥 Max Probability", value=f"{filtered_df['fraud_probability'].max():.2f}", delta_color="off")
with col4:
    st.metric(label="🕒 Most Recent Alert", value=filtered_df['timestamp'].max().strftime("%Y-%m-%d %H:%M") if not filtered_df.empty else "N/A", delta_color="off")
style_metric_cards(background_color="#1a1a1a", border_color="#2e2e2e", border_size_px=2)

# === Data Summary Expandable Section ===
if st.session_state.get('show_summary', False):
    with st.expander("📊 Dataset Summary", expanded=True):
        summary_col1, summary_col2 = st.columns(2)
        with summary_col1:
            st.write("**Numerical Summary**")
            st.dataframe(filtered_df[['amount', 'fraud_probability']].describe().style.format("{:.2f}"))
        with summary_col2:
            st.write("**Categorical Summary**")
            st.dataframe(pd.DataFrame({
                'Merchant Category': filtered_df['merchant_category'].value_counts().index,
                'Count': filtered_df['merchant_category'].value_counts().values
            }))

# === Main Dashboard Sections ===
tab1, tab2, tab3, tab4 = st.tabs(["📈 Trends Analysis", "⚖️ Feature Impact", "🔍 Transaction Details", "🗃️ Raw Data"])

with tab1:
    st.header("Temporal Fraud Patterns")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Fraud Probability Distribution")
        fig1 = px.histogram(filtered_df, x="fraud_probability", nbins=20, 
                          color_discrete_sequence=['#ff4b4b'],
                          labels={'fraud_probability': 'Fraud Probability'},
                          template='plotly_dark')
        fig1.update_layout(bargap=0.1)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.subheader("Fraud Timeline Analysis")
        df_time = filtered_df.copy()
        df_time['date'] = df_time['timestamp'].dt.floor('H')
        time_count = df_time.groupby('date').size().reset_index(name='count')
        fig2 = px.area(time_count, x='date', y='count', 
                       labels={'date': 'Time', 'count': 'Fraud Count'},
                       color_discrete_sequence=['#ff4b4b'],
                       template='plotly_dark')
        st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.header("Feature Impact Analysis")
    
    # Global SHAP Analysis
    st.subheader("Global Feature Importance")
    all_shap_features = {}
    for _, row in filtered_df.iterrows():
        shap_dict = row['shap_dict']
        for feature, value in shap_dict.items():
            if feature not in all_shap_features:
                all_shap_features[feature] = []
            all_shap_features[feature].append(abs(value))
    
    if all_shap_features:
        mean_shap_values = {k: np.mean(v) for k, v in all_shap_features.items()}
        shap_df = pd.DataFrame({
            'Feature': list(mean_shap_values.keys()),
            'Importance': list(mean_shap_values.values())
        }).sort_values('Importance', ascending=False).head(15)
        
        fig3 = px.bar(shap_df, x='Importance', y='Feature', orientation='h',
                     color='Importance', color_continuous_scale='RdYlGn_r',
                     template='plotly_dark')
        fig3.update_layout(height=600)
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No SHAP data available for current filters")
    
    # SHAP Correlation Matrix
    st.subheader("Feature Interaction Network")
    if all_shap_features and len(mean_shap_values) > 1:
        top_features = list(mean_shap_values.keys())[:10]
        correlation_matrix = filtered_df[[f for f in top_features if f in filtered_df.columns]].corr()
        fig4 = px.imshow(correlation_matrix,
                        labels=dict(x="Feature", y="Feature", color="Correlation"),
                        color_continuous_scale='RdBu_r',
                        template='plotly_dark')
        st.plotly_chart(fig4, use_container_width=True)

with tab3:
    st.header("Transaction-Level Investigation")
    
    if not filtered_df.empty:
        selected_transaction = st.selectbox("Select Transaction", filtered_df['transaction_id'])
        transaction_data = filtered_df[filtered_df['transaction_id'] == selected_transaction].iloc[0]
        
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Transaction Metadata")
            st.metric("Amount", f"${transaction_data['amount']:,.2f}")
            st.metric("Fraud Probability", f"{transaction_data['fraud_probability']:.2%}")
            st.write(f"**Merchant:** {transaction_data['merchant_category']}")
            st.write(f"**Timestamp:** {transaction_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        with col2:
            st.subheader("SHAP Value Breakdown")
            if transaction_data['shap_dict']:
                shap_df = pd.DataFrame({
                    'Feature': list(transaction_data['shap_dict'].keys()),
                    'Impact': list(transaction_data['shap_dict'].values())
                }).sort_values('Impact', ascending=False)
                
                fig5 = px.bar(shap_df.head(10), x='Impact', y='Feature', orientation='h',
                             color='Impact', color_continuous_scale='RdYlGn_r',
                             template='plotly_dark')
                st.plotly_chart(fig5, use_container_width=True)
            else:
                st.warning("No SHAP data available for this transaction")

with tab4:
    st.header("Raw Transaction Data")
    st.dataframe(filtered_df.style.format({
        'amount': '${:,.2f}',
        'fraud_probability': '{:.2%}',
        'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
    }), use_container_width=True, height=600)

# === Footer ===
st.markdown("---")
st.markdown("🔒 **Fraud Detection System v2.1** | 📊 Dashboard updated every 5 minutes | 🚨 Alert Threshold: {:.0%}".format(prob_threshold))