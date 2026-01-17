"""
Streamlit Entry Point - Auto-redirects to Agentic Issues
"""
import streamlit as st

# Minimal config
st.set_page_config(
    page_title="Data Quality Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Immediately redirect - users should never see this page
st.switch_page("pages/2_Agentic_Issues.py")
