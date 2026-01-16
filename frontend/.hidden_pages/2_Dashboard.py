"""
Dashboard Page - Source-Filtered Results from Storage
"""
import streamlit as st
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dq_engine.storage import StorageFactory

load_dotenv()

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")

st.title("📊 Data Quality Dashboard")
st.markdown("View validation results filtered by data source")

# Source type selector - Only show implemented sources
st.sidebar.markdown("### Filter by Source")
st.sidebar.info("📌 Currently showing: **S3 only**\n\nDatabricks & Snowflake coming soon!")

source_type = 's3'  # Only S3 implemented for now
st.sidebar.success("☁️ AWS S3 (Active)")

# Get storage for selected source
try:
    storage = StorageFactory.get_storage(source_type)
    
    # Test connection
    conn_ok, conn_msg = storage.test_connection()
    if not conn_ok:
        st.error(f"❌ {conn_msg}")
        st.stop()
    
    # List all sources of this type
    sources = storage.list_sources()
    
    # Debug: Show what we found
    st.sidebar.write(f"Debug: Found {len(sources)} sources")
    if sources:
        st.sidebar.write("Sources:", sources)
    
    if not sources:
        st.warning(f"🔍 No validation results found for {source_type.upper()} sources!")
        st.info(f"""
        **To see results:**
        1. Configure a {source_type.upper()} dataset in the Configure page
        2. Run validation: `cd backend && source venv/bin/activate && cd .. && python3 run_validation_simple.py`
        3. Results will appear here automatically
        
        **Note:** Only showing results for **{source_type.upper()}** sources. 
        Select different source type in sidebar to view other sources.
        """)
        st.stop()
    
    # Dataset selector
    st.markdown(f"### Select {source_type.upper()} Dataset")
    selected_source = st.selectbox(
        f"Found {len(sources)} {source_type} source(s):",
        sources,
        key="source_selector"
    )
    
    # Get latest results
    dataset_data = storage.get_latest(selected_source)
    
    if not dataset_data:
        st.error(f"Could not load results for {selected_source}")
        st.stop()
    
    # Display metrics
    st.markdown("---")
    st.markdown(f"## {dataset_data.get('dataset', selected_source)}")
    st.caption(f"📍 Source: {dataset_data.get('source', selected_source)}")
    st.caption(f"🕐 Last validated: {dataset_data.get('timestamp', 'Unknown')}")
    st.caption(f"📊 Source Type: **{source_type.upper()}**")
    
    # Quality Score
    col1, col2, col3, col4 = st.columns(4)
    
    summary = dataset_data.get('summary', {})
    
    with col1:
        st.metric(
            "Quality Score",
            f"{summary.get('quality_score', 0)}%",
        )
    
    with col2:
        st.metric(
            "Total Checks",
            summary.get('total_checks', 0)
        )
    
    with col3:
        st.metric(
            "Passed",
            summary.get('passed', 0)
        )
    
    with col4:
        st.metric(
            "Failed",
            summary.get('failed', 0),
            delta=-summary.get('failed', 0) if summary.get('failed', 0) > 0 else None
        )
    
    # Detailed Results
    st.markdown("---")
    st.markdown("### 📋 Detailed Check Results")
    
    results = dataset_data.get('results', {})
    
    # Null Check
    null_check = results.get('null_check', {})
    with st.expander(
        f"{'❌' if null_check.get('status') == 'FAIL' else '✅'} Null Check", 
        expanded=(null_check.get('status') == 'FAIL')
    ):
        st.write(f"**Status:** {null_check.get('status', 'UNKNOWN')}")
        st.write(f"**Total Nulls:** {null_check.get('total_nulls', 0)}")
        if null_check.get('failed_columns'):
            st.write(f"**Columns with Nulls:** {', '.join(null_check['failed_columns'])}")
    
    # Duplicate Check
    dup_check = results.get('duplicate_check', {})
    with st.expander(
        f"{'✅' if dup_check.get('status') == 'PASS' else '❌'} Duplicate Check"
    ):
        st.write(f"**Status:** {dup_check.get('status', 'UNKNOWN')}")
        st.write(f"**Duplicate Count:** {dup_check.get('duplicate_count', 0)}")
        st.write(f"**Duplicate %:** {dup_check.get('duplicate_percentage', 0)}%")
    
    # Freshness Check
    fresh_check = results.get('freshness_check', {})
    with st.expander(
        f"{'✅' if fresh_check.get('status') == 'PASS' else '❌'} Freshness Check"
    ):
        st.write(f"**Status:** {fresh_check.get('status', 'UNKNOWN')}")
        st.write(f"**Latest Timestamp:** {fresh_check.get('latest_timestamp', 'N/A')}")
        st.write(f"**Age (hours):** {fresh_check.get('age_hours', 0):.2f}")
    
    # Volume Check
    vol_check = results.get('volume_check', {})
    with st.expander(f"Volume Check - {vol_check.get('status', 'UNKNOWN')}"):
        st.write(f"**Status:** {vol_check.get('status', 'UNKNOWN')}")
        st.write(f"**Current Count:** {vol_check.get('current_count', 0):,} rows")
        if vol_check.get('message'):
            st.info(vol_check['message'])
    
    # Row count info
    st.markdown("---")
    st.info(f"📊 Total rows validated: **{dataset_data.get('row_count', 0):,}**")
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔄 Refresh Results"):
            st.rerun()
    with col2:
        # Get historical data
        try:
            history = storage.get_history(selected_source, days=7)
            if history:
                st.success(f"📈 {len(history)} validation runs in last 7 days")
        except:
            pass
    with col3:
        if source_type == 's3':
            bucket = dataset_data.get('source', '').replace('s3://', '').split('/')[0]
            st.markdown(f"[View in S3 Console ↗](https://s3.console.aws.amazon.com/s3/buckets/{bucket})")

except ValueError as e:
    st.error(f"❌ {str(e)}")
    st.info("Please select a supported source type from the sidebar")
except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    import traceback
    st.code(traceback.format_exc())
