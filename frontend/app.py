"""
Streamlit Frontend - Main Application
Data Quality Platform
"""
import streamlit as st
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="Data Quality Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/150x50/1f77b4/ffffff?text=DQ+Platform")
    st.markdown("---")
    
    st.markdown("### 🔍 Data Quality Platform")
    st.markdown("AI-powered data quality automation")
    
    st.markdown("---")
    
    # Navigation
    st.markdown("### Navigation")
    st.markdown("- 🏠 **Home** (current page)")
    st.markdown("- ⚙️ Configure Dataset → Go to pages dropdown")
    st.markdown("- 📊 Dashboard → Go to pages dropdown")
    st.markdown("- 💬 AI Chatbot → Go to pages dropdown")
    
    st.markdown("---")
    
    # Backend status
    st.markdown("### System Status")
    try:
        response = requests.get(f"{BACKEND_URL}/api/health", timeout=2)
        if response.status_code == 200:
            st.success("✅ Backend Online")
        else:
            st.error("❌ Backend Error")
    except:
        st.error("❌ Backend Offline")

    st.markdown("---")
    st.markdown("### 🔗 Quick Links")
    st.markdown("[📊 Airflow UI](http://localhost:8080)")
    st.caption("View DAGs and schedules")

# Main content
st.markdown('<div class="main-header">🔍 Data Quality Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Automated data quality validation with AI-powered insights</div>', unsafe_allow_html=True)

# Welcome section
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### ⚙️ Configure")
    st.markdown("""
    Set up your data sources and define quality checks:
    - Select data source (S3, Snowflake, CSV)
    - Define schema and primary keys
    - Choose quality checks
    - Submit for validation
    """)
    if st.button("➡️ Go to Configuration"):
        st.switch_page("pages/1_Configure.py")

with col2:
    st.markdown("### 📊 Monitor")
    st.markdown("""
    View quality metrics and results:
    - Real-time validation status
    - Historical trends
    - AI-generated insights
    - Anomaly detection
    """)
    if st.button("➡️ Go to Dashboard"):
        st.switch_page("pages/2_Dashboard.py")

with col3:
    st.markdown("### 💬 Ask AI")
    st.markdown("""
    Get answers in natural language:
    - "Where are null values?"
    - "Is today's data normal?"
    - "Should I block pipelines?"
    - "What changed vs yesterday?"
    """)
    if st.button("➡️ Go to Chatbot"):
        st.switch_page("pages/3_Chatbot.py")

st.markdown("---")

# Quick stats from S3
st.markdown("### 📈 Quick Stats")

try:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from dq_engine.storage import S3Storage
    
    storage = S3Storage()
    sources = storage.list_sources()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Datasets", len(sources))
    
    with col2:
        # Count recent validations (last 24h)
        from datetime import datetime, timedelta
        recent_count = 0
        cutoff = datetime.now() - timedelta(hours=24)
        for source in sources:
            data = storage.get_latest(source)
            if data:
                try:
                    timestamp = datetime.fromisoformat(data.get('timestamp', ''))
                    if timestamp >= cutoff:
                        recent_count += 1
                except:
                    pass
        st.metric("Last 24h", recent_count)
    
    with col3:
        st.metric("Data Sources", "1 (S3)")
    
    with col4:
        st.metric("Quality Checks", "4 Types")
    
    # Recent validations
    if sources:
        st.markdown("### 📋 Recent Validations")
        recent_sources = sources[:5]
        for source in recent_sources:
            data = storage.get_latest(source)
            if data:
                score = data.get('summary', {}).get('quality_score', 0)
                emoji = "🟢" if score >= 75 else "🟡" if score >= 50 else "🔴"
                with st.expander(f"{emoji} {data.get('dataset', source)}"):
                    st.write(f"**Source:** {data.get('source', 'N/A')}")
                    st.write(f"**Quality Score:** {score}%")
                    st.write(f"**Last Validated:** {data.get('timestamp', 'N/A')[:19]}")
                    st.write(f"**Rows:** {data.get('row_count', 0):,}")
    else:
        st.info("🔍 No datasets validated yet. Go to Configuration to run your first validation!")
        
except Exception as e:
    st.warning("📊 Quick stats will appear here after you run validations")
    st.info("Go to **Configure** page → Select a file → Click 'Run Validation'")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem 0;'>
    <p>Built with ❤️ for Hackathon 2026</p>
    <p>Powered by Airflow • FastAPI • Streamlit • OpenAI</p>
</div>
""", unsafe_allow_html=True)
