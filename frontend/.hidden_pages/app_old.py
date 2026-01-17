"""
Streamlit Frontend - Main Application (Hidden - Auto-redirects)
"""
import streamlit as st

# Page config - set to hide from navigation by using a different approach
st.set_page_config(
    page_title="Data Quality Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=None  # Hide menu
)

# Immediately redirect to Agentic Issues - this page should not be visible
st.switch_page("pages/2_Agentic_Issues.py")
