"""
Agentic Data Quality Issues Page
Shows the issue matrix and drill-down views for agentic data quality fixes
"""
import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

def convert_units_frontend(value: float, from_unit: str, to_unit: str) -> float:
    """Simple unit conversion for frontend display"""
    # Conversion factors to cm (base unit for length)
    to_cm = {
        'cm': 1.0,
        'm': 100.0,
        'in': 2.54,
        'ft': 30.48,
    }
    # Conversion factors to kg (base unit for weight)
    to_kg = {
        'kg': 1.0,
        'g': 0.001,
        'lb': 0.453592,
        'oz': 0.0283495,
    }
    
    # Determine if length or weight
    length_units = ['cm', 'm', 'in', 'ft']
    weight_units = ['kg', 'g', 'lb', 'oz']
    
    if from_unit in length_units and to_unit in length_units:
        # Convert to cm first
        cm_value = value * to_cm.get(from_unit, 1.0)
        # Convert from cm to target
        return cm_value / to_cm.get(to_unit, 1.0)
    elif from_unit in weight_units and to_unit in weight_units:
        # Convert to kg first
        kg_value = value * to_kg.get(from_unit, 1.0)
        # Convert from kg to target
        return kg_value / to_kg.get(to_unit, 1.0)
    
    return None

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
DEFAULT_S3_BUCKET = os.getenv("DQ_SOURCE_BUCKET", os.getenv("DQ_RESULTS_BUCKET", "project-cb"))

st.set_page_config(page_title="Agentic Issues", page_icon="🤖", layout="wide", initial_sidebar_state="expanded")

# 🎨 MODERN DARK THEME WITH ANIMATIONS
st.markdown("""
<style>
    /* Dark Theme Base */
    .stApp {
        background: linear-gradient(135deg, #0f0f1e 0%, #1a1a2e 50%, #16213e 100%);
        color: #e0e0e0;
    }
    
    /* Animated Gradient Title */
    .main-title {
        font-size: 3.5rem;
        font-weight: 900;
        background: linear-gradient(45deg, #00f5ff, #ff00ff, #00ff88, #ffaa00);
        background-size: 300% 300%;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        animation: gradient-shift 3s ease infinite;
        text-align: center;
        margin-bottom: 0.5rem;
        text-shadow: 0 0 30px rgba(0, 245, 255, 0.5);
    }
    
    @keyframes gradient-shift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Subtitle with glow */
    .subtitle {
        text-align: center;
        color: #00f5ff;
        font-size: 1.2rem;
        margin-bottom: 2rem;
        text-shadow: 0 0 10px rgba(0, 245, 255, 0.8);
        animation: pulse-glow 2s ease-in-out infinite;
    }
    
    @keyframes pulse-glow {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    /* Card Styles with Glassmorphism */
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(0, 245, 255, 0.3);
        border-radius: 15px;
        padding: 1rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
    }
    
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px) scale(1.02);
        border-color: rgba(0, 245, 255, 0.8);
        box-shadow: 0 12px 40px rgba(0, 245, 255, 0.4);
    }
    
    /* Button Animations */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 50px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton>button:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.6);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    /* Data Editor Styling */
    .stDataFrame {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 15px;
        border: 1px solid rgba(0, 245, 255, 0.2);
        overflow: hidden;
        animation: fadeIn 0.5s ease-in;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #0f0f1e 100%);
        border-right: 1px solid rgba(0, 245, 255, 0.2);
    }
    
    section[data-testid="stSidebar"] .stSelectbox {
        background: rgba(26, 26, 46, 0.9) !important;
        border-radius: 10px !important;
        border: 1px solid rgba(0, 245, 255, 0.4) !important;
    }
    
    /* Success/Error Messages */
    .stSuccess {
        background: rgba(0, 255, 136, 0.1);
        border-left: 4px solid #00ff88;
        border-radius: 10px;
        animation: slideIn 0.5s ease;
    }
    
    .stError {
        background: rgba(255, 0, 85, 0.1);
        border-left: 4px solid #ff0055;
        border-radius: 10px;
        animation: slideIn 0.5s ease;
    }
    
    @keyframes slideIn {
        from { transform: translateX(-100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    
    /* Loading Spinner */
    .stSpinner {
        color: #00f5ff;
    }
    
    /* Info boxes with neon glow */
    .stInfo {
        background: rgba(0, 245, 255, 0.1);
        border: 1px solid rgba(0, 245, 255, 0.3);
        border-radius: 10px;
        box-shadow: 0 0 20px rgba(0, 245, 255, 0.2);
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255, 255, 255, 0.02);
        border-radius: 10px;
        padding: 0.5rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: #00f5ff;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(0, 245, 255, 0.1);
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Download button special style */
    .stDownloadButton>button {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        animation: pulse 2s ease-in-out infinite;
    }
    
    @keyframes pulse {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.05); }
    }
    
    /* Hide top white bar */
    header[data-testid="stHeader"] {
        display: none !important;
    }
    
    /* Style all input fields to dark theme */
    .stTextInput>div>div>input,
    .stTextInput>div>div>input:focus {
        background-color: rgba(26, 26, 46, 0.8) !important;
        color: #00f5ff !important;
        border: 1px solid rgba(0, 245, 255, 0.3) !important;
        border-radius: 8px !important;
        padding: 0.5rem !important;
    }
    
    .stTextInput>div>div>input::placeholder {
        color: rgba(0, 245, 255, 0.5) !important;
    }
    
    /* Style selectbox dropdowns - Dark theme */
    .stSelectbox>div>div {
        background-color: rgba(26, 26, 46, 0.9) !important;
        color: #00f5ff !important;
        border: 1px solid rgba(0, 245, 255, 0.4) !important;
        border-radius: 8px !important;
    }
    
    .stSelectbox>div>div>div {
        color: #00f5ff !important;
        background-color: rgba(26, 26, 46, 0.9) !important;
    }
    
    /* Selectbox dropdown menu (the list that appears) - AGGRESSIVE DARK THEME */
    .stSelectbox [role="listbox"],
    .stSelectbox [role="option"],
    .stSelectbox ul,
    .stSelectbox li,
    div[data-baseweb="select"] [role="listbox"],
    div[data-baseweb="select"] [role="option"],
    div[data-baseweb="select"] ul,
    div[data-baseweb="select"] li,
    div[data-baseweb="popover"],
    div[data-baseweb="popover"] ul,
    div[data-baseweb="popover"] li,
    div[data-baseweb="popover"] [role="option"],
    [data-baseweb="select"] + div,
    [data-baseweb="select"] + div ul,
    [data-baseweb="select"] + div li {
        background-color: rgba(26, 26, 46, 0.98) !important;
        color: #00f5ff !important;
        border: 1px solid rgba(0, 245, 255, 0.4) !important;
    }
    
    /* Selected option in dropdown */
    .stSelectbox [role="option"][aria-selected="true"],
    div[data-baseweb="select"] [role="option"][aria-selected="true"],
    div[data-baseweb="popover"] [role="option"][aria-selected="true"] {
        background-color: rgba(0, 245, 255, 0.3) !important;
        color: #ffffff !important;
    }
    
    /* Hover state in dropdown */
    .stSelectbox [role="option"]:hover,
    div[data-baseweb="select"] [role="option"]:hover,
    div[data-baseweb="popover"] [role="option"]:hover,
    .stSelectbox li:hover,
    div[data-baseweb="select"] li:hover {
        background-color: rgba(0, 245, 255, 0.2) !important;
        color: #ffffff !important;
    }
    
    /* Selectbox input text */
    .stSelectbox input,
    .stSelectbox [data-baseweb="select"] input {
        background-color: rgba(26, 26, 46, 0.9) !important;
        color: #00f5ff !important;
    }
    
    /* Force dark theme on ALL dropdown containers */
    div[data-baseweb="popover"] {
        background-color: rgba(26, 26, 46, 0.98) !important;
        border: 1px solid rgba(0, 245, 255, 0.4) !important;
    }
    
    /* Override any white backgrounds in selectbox */
    .stSelectbox *,
    div[data-baseweb="select"] *,
    div[data-baseweb="popover"] * {
        background-color: rgba(26, 26, 46, 0.98) !important;
    }
    
    /* Exception: Only selected/hovered items get highlight */
    .stSelectbox [role="option"][aria-selected="true"],
    .stSelectbox [role="option"]:hover,
    div[data-baseweb="select"] [role="option"][aria-selected="true"],
    div[data-baseweb="select"] [role="option"]:hover {
        background-color: rgba(0, 245, 255, 0.25) !important;
    }
    
    /* Style slider */
    .stSlider>div>div {
        background-color: rgba(26, 26, 46, 0.5) !important;
    }
    
    .stSlider>div>div>div>div {
        background: linear-gradient(90deg, #00f5ff, #ff00ff) !important;
    }
    
    /* Navigation Links - Remove all overlays and top borders */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
        background: transparent !important;
    }
    
    /* Remove any top borders, overlays, or pseudo-elements from nav items */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] *::before,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] *::after {
        display: none !important;
        content: none !important;
    }
    
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] div:not([aria-current="page"]),
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] span:not([aria-current="page"]),
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] > div > div:not([aria-current="page"]) {
        color: #00f5ff !important;
        font-weight: 700 !important;
        text-shadow: none !important;
        font-size: 1rem !important;
        border-top: none !important;
        border: none !important;
        box-shadow: none !important;
    }
    
    /* Hover state - NO glows, just background and color change */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] div:not([aria-current="page"]):hover {
        color: #ff00ff !important;
        text-shadow: none !important;
        background: rgba(255, 0, 255, 0.1) !important;
        border: none !important;
        box-shadow: none !important;
    }
    
    /* Active navigation link - ONLY background gradient, REMOVE ALL OVERLAYS AND NESTED BACKGROUNDS */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"],
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] a,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] div,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] span {
        background: linear-gradient(135deg, rgba(0, 245, 255, 0.3), rgba(255, 0, 255, 0.3)) !important;
        border: none !important;
        border-radius: 8px !important;
        color: #ffffff !important;
        font-weight: 800 !important;
        text-shadow: none !important;
        box-shadow: none !important;
        isolation: isolate !important;
        overflow: hidden !important;
        position: relative !important;
    }
    
    /* REMOVE ALL BACKGROUNDS FROM CHILD ELEMENTS - THIS IS THE OVERLAY! */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] * {
        background: transparent !important;
        background-color: transparent !important;
        box-shadow: none !important;
        text-shadow: none !important;
    }
    
    /* Keep text color on children */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] * {
        color: inherit !important;
    }
    
    /* Remove ALL pseudo-elements that could create overlays */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"]::before,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"]::after,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] *::before,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] *::after,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] a::before,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] a::after,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] div::before,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] [aria-current="page"] div::after {
        display: none !important;
        content: none !important;
        background: none !important;
        background-color: transparent !important;
    }
    
    /* Prevent glow overlap between navigation items */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] > div > div {
        margin-bottom: 4px !important;
        isolation: isolate !important;
        overflow: hidden !important;
        position: relative !important;
    }
    
    /* Ensure unselected items don't have glows that bleed */
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] > div > div:not([aria-current="page"]) {
        box-shadow: none !important;
        isolation: isolate !important;
        overflow: hidden !important;
    }
    
    /* Fix info box text color */
    .stInfo {
        background: linear-gradient(135deg, rgba(0, 245, 255, 0.15), rgba(102, 126, 234, 0.15)) !important;
        border: 1px solid rgba(0, 245, 255, 0.3) !important;
        color: #ffffff !important;
    }
    
    .stInfo p, .stInfo div, .stInfo strong {
        color: #ffffff !important;
    }
    
    /* Style warning boxes */
    .stWarning {
        background: linear-gradient(135deg, rgba(255, 170, 0, 0.15), rgba(255, 136, 0, 0.15)) !important;
        border: 1px solid rgba(255, 170, 0, 0.3) !important;
        color: #ffffff !important;
    }
    
    .stWarning p, .stWarning div, .stWarning strong {
        color: #ffffff !important;
    }
    
    /* Style checkbox */
    .stCheckbox>label {
        color: #00f5ff !important;
        font-weight: 600 !important;
    }
    
    /* Make all text in main area visible */
    .main .block-container {
        color: #e0e0e0 !important;
    }
    
    .main .block-container p, .main .block-container div {
        color: #e0e0e0 !important;
    }
    
    /* DARK THEME FOR ALL TABLES (st.dataframe AND st.data_editor) */
    /* Table container */
    div[data-testid="stDataFrameContainer"],
    div[data-testid="stDataFrame"],
    div[data-testid="stDataEditor"],
    div[data-testid="stDataEditorContainer"] {
        background: rgba(26, 26, 46, 0.8) !important;
        border: 1px solid rgba(0, 245, 255, 0.3) !important;
        border-radius: 10px !important;
        padding: 1rem !important;
    }
    
    /* Table itself */
    table,
    div[data-testid="stDataEditor"] table,
    div[data-testid="stDataFrame"] table {
        background: rgba(26, 26, 46, 0.9) !important;
        color: #e0e0e0 !important;
        border-collapse: collapse !important;
        width: 100% !important;
    }
    
    /* Table headers */
    table thead th,
    table thead tr th,
    div[data-testid="stDataEditor"] table thead th,
    div[data-testid="stDataFrame"] table thead th {
        background: rgba(0, 245, 255, 0.2) !important;
        color: #00f5ff !important;
        border: 1px solid rgba(0, 245, 255, 0.3) !important;
        padding: 0.75rem !important;
        font-weight: 700 !important;
        text-align: left !important;
    }
    
    /* Table body cells */
    table tbody td,
    table tbody tr td,
    div[data-testid="stDataEditor"] table tbody td,
    div[data-testid="stDataFrame"] table tbody td {
        background: rgba(26, 26, 46, 0.9) !important;
        color: #e0e0e0 !important;
        border: 1px solid rgba(0, 245, 255, 0.1) !important;
        padding: 0.75rem !important;
    }
    
    /* Alternating row colors */
    table tbody tr:nth-child(even),
    div[data-testid="stDataEditor"] table tbody tr:nth-child(even),
    div[data-testid="stDataFrame"] table tbody tr:nth-child(even) {
        background: rgba(0, 245, 255, 0.05) !important;
    }
    
    table tbody tr:nth-child(odd),
    div[data-testid="stDataEditor"] table tbody tr:nth-child(odd),
    div[data-testid="stDataFrame"] table tbody tr:nth-child(odd) {
        background: rgba(26, 26, 46, 0.9) !important;
    }
    
    /* Hover effect on rows */
    table tbody tr:hover,
    div[data-testid="stDataEditor"] table tbody tr:hover,
    div[data-testid="stDataFrame"] table tbody tr:hover {
        background: rgba(0, 245, 255, 0.15) !important;
        transform: scale(1.01);
        transition: all 0.2s ease;
    }
    
    /* Checkbox styling in tables */
    table input[type="checkbox"],
    div[data-testid="stDataEditor"] input[type="checkbox"],
    div[data-testid="stDataFrame"] input[type="checkbox"] {
        accent-color: #00f5ff !important;
        width: 18px !important;
        height: 18px !important;
        cursor: pointer !important;
    }
    
    /* Styled dataframe/data editor wrapper */
    .stDataFrame > div,
    .stDataEditor > div,
    div[data-testid="stDataEditor"] > div {
        background: rgba(26, 26, 46, 0.8) !important;
    }
    
    /* Force dark theme on all table-related elements */
    .stDataFrame *,
    .stDataEditor *,
    div[data-testid="stDataFrameContainer"] *,
    div[data-testid="stDataFrame"] *,
    div[data-testid="stDataEditor"] *,
    div[data-testid="stDataEditorContainer"] * {
        color: #e0e0e0 !important;
    }
    
    /* Exception: Headers should be cyan */
    .stDataFrame thead *,
    .stDataEditor thead *,
    div[data-testid="stDataFrameContainer"] thead *,
    div[data-testid="stDataEditor"] thead * {
        color: #00f5ff !important;
    }
    
    /* Data editor specific - editable cells */
    div[data-testid="stDataEditor"] input,
    div[data-testid="stDataEditor"] textarea {
        background: rgba(26, 26, 46, 0.9) !important;
        color: #e0e0e0 !important;
        border: 1px solid rgba(0, 245, 255, 0.3) !important;
    }
    
    /* BaseWeb table styling override for dark theme */
    [data-baseweb="table"],
    [data-baseweb="table"] table,
    [data-baseweb="table"] thead,
    [data-baseweb="table"] tbody,
    [data-baseweb="table"] th,
    [data-baseweb="table"] td {
        background: rgba(26, 26, 46, 0.9) !important;
        color: #e0e0e0 !important;
        border-color: rgba(0, 245, 255, 0.2) !important;
    }
    
    [data-baseweb="table"] thead th {
        background: rgba(0, 245, 255, 0.2) !important;
        color: #00f5ff !important;
    }
    
    /* Progress Animation Container - HORIZONTAL LAYOUT */
    .progress-container {
        background: linear-gradient(135deg, rgba(0, 245, 255, 0.1), rgba(255, 0, 255, 0.1));
        border: 2px solid rgba(0, 245, 255, 0.3);
        border-radius: 15px;
        padding: 1.5rem 2rem;
        margin: 2rem 0;
        box-shadow: 0 8px 32px rgba(0, 245, 255, 0.2);
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    
    .progress-header {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.75rem;
        margin-bottom: 1.5rem;
        width: 100%;
    }
    
    .progress-text {
        color: #00f5ff;
        font-size: 1.3rem;
        font-weight: 700;
        text-shadow: 0 0 10px rgba(0, 245, 255, 0.8);
        animation: pulse-glow 2s ease-in-out infinite;
        margin: 0;
    }
    
    .progress-bar-container {
        width: 100%;
        height: 8px;
        background: rgba(26, 26, 46, 0.8);
        border-radius: 10px;
        overflow: hidden;
        margin-bottom: 1.5rem;
        border: 1px solid rgba(0, 245, 255, 0.3);
        position: relative;
    }
    
    .progress-bar {
        height: 100%;
        background: linear-gradient(90deg, #00f5ff, #ff00ff, #00ff88, #ffaa00);
        background-size: 300% 100%;
        animation: progress-flow 2s linear infinite, gradient-shift 3s ease infinite;
        border-radius: 10px;
        position: relative;
        overflow: hidden;
    }
    
    @keyframes progress-flow {
        0% { width: 0%; }
        50% { width: 70%; }
        100% { width: 100%; }
    }
    
    .progress-steps {
        display: flex;
        justify-content: space-between;
        align-items: center;
        width: 100%;
        gap: 1rem;
        flex-wrap: nowrap;
    }
    
    .progress-step {
        flex: 1;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        color: #b0b0b0;
        font-size: 0.85rem;
        padding: 1rem 0.75rem;
        background: rgba(26, 26, 46, 0.5);
        border-radius: 12px;
        border: 1px solid rgba(0, 245, 255, 0.2);
        transition: all 0.3s ease;
        min-width: 120px;
        position: relative;
    }
    
    .progress-step::after {
        content: '→';
        position: absolute;
        right: -1.2rem;
        color: rgba(0, 245, 255, 0.4);
        font-size: 1.5rem;
        font-weight: bold;
    }
    
    .progress-step:last-child::after {
        display: none;
    }
    
    .progress-step-icon {
        font-size: 1.8rem;
        margin-bottom: 0.5rem;
        filter: drop-shadow(0 0 5px rgba(0, 245, 255, 0.3));
    }
    
    .progress-step.active {
        color: #00f5ff;
        background: rgba(0, 245, 255, 0.2);
        border-color: rgba(0, 245, 255, 0.5);
        box-shadow: 0 0 20px rgba(0, 245, 255, 0.4);
        animation: pulse-glow 1.5s ease-in-out infinite;
        transform: scale(1.05);
    }
    
    .progress-step.active .progress-step-icon {
        filter: drop-shadow(0 0 10px rgba(0, 245, 255, 0.8));
        animation: bounce 1s ease-in-out infinite;
    }
    
    @keyframes bounce {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-5px); }
    }
</style>
""", unsafe_allow_html=True)

# Animated Title
st.markdown('<h1 class="main-title">🤖 AGENTIC DQ PLATFORM</h1>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">✨ AI-Powered Smart Data Fixes • Real-time Issue Detection • Zero Code Required ✨</p>', unsafe_allow_html=True)

# Store the latest validation results locally (no history / no caching)
if "agentic_latest_results" not in st.session_state:
    st.session_state.agentic_latest_results = None
if "agentic_latest_results_dataset" not in st.session_state:
    st.session_state.agentic_latest_results_dataset = None
if "agentic_latest_source_bucket" not in st.session_state:
    st.session_state.agentic_latest_source_bucket = None
if "agentic_latest_source_key" not in st.session_state:
    st.session_state.agentic_latest_source_key = None
if "validation_in_progress" not in st.session_state:
    st.session_state.validation_in_progress = False

# Unit preference for standardization (can be set per-column in future)
if "unit_preferences" not in st.session_state:
    st.session_state.unit_preferences = {
        "height": "cm",
        "length": "cm", 
        "weight": "kg",
        "distance": "cm"
    }

# Sidebar for S3 file selection and validation trigger
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem; background: linear-gradient(135deg, rgba(102, 126, 234, 0.2), rgba(118, 75, 162, 0.2)); border-radius: 15px; margin-bottom: 1.5rem; border: 1px solid rgba(0, 245, 255, 0.3);">
        <h2 style="color: #00f5ff; margin: 0; font-size: 1.8rem;">📁 S3 FILE BROWSER</h2>
        <p style="color: #b0b0b0; font-size: 0.9rem; margin-top: 0.5rem;">Select your data source</p>
    </div>
    """, unsafe_allow_html=True)

    # Remember selections across reruns
    if "agentic_selected_dataset" not in st.session_state:
        st.session_state.agentic_selected_dataset = None
    if "agentic_selected_validation_id" not in st.session_state:
        st.session_state.agentic_selected_validation_id = None

    st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; font-size: 0.95rem;">🪣 S3 BUCKET</p>', unsafe_allow_html=True)
    bucket = st.text_input("S3 Bucket", value=DEFAULT_S3_BUCKET, key="s3_bucket", label_visibility="collapsed")
    
    st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; margin-top: 1rem; font-size: 0.95rem;">📂 PREFIX (OPTIONAL)</p>', unsafe_allow_html=True)
    prefix = st.text_input("Prefix (optional)", value="", key="s3_prefix", label_visibility="collapsed")

    s3_files = []
    if bucket:
        try:
            resp = requests.get(
                f"{BACKEND_URL}/api/s3/list-files",
                params={"bucket": bucket, "prefix": prefix},
                timeout=30,  # give backend more time to talk to S3
            )
            if resp.status_code == 200:
                s3_files = resp.json().get("files", [])
            else:
                st.error(f"Error listing S3 files: {resp.status_code}")
        except requests.exceptions.Timeout:
            st.error("S3 listing timed out after 30 seconds. Try using a more specific prefix to reduce results.")
        except Exception as e:
            st.error(f"Error connecting to backend for S3 listing: {e}")

    selected_dataset = None
    selected_validation_id = None

    # Filters (shown above run validation)
    st.markdown("""
    <div style="margin-top: 2rem; margin-bottom: 1rem;">
        <h3 style="color: #ff00ff; font-size: 1.3rem; font-weight: 700; text-shadow: 0 0 10px rgba(255, 0, 255, 0.5);">
            🎛️ FILTERS
        </h3>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; font-size: 0.95rem;">📁 CATEGORY</p>', unsafe_allow_html=True)
    filter_category = st.selectbox(
        "Category:",
        ["All", "Semantic", "Logic", "Formatting", "Imputation", "Extraction", "Categorical", "Units"],
        key="filter_category",
        label_visibility="collapsed"
    )
    
    st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; margin-top: 1rem; font-size: 0.95rem;">📊 MIN CONFIDENCE</p>', unsafe_allow_html=True)
    filter_confidence = st.slider(
        "Min Confidence:", 0.0, 1.0, 0.0, 0.1, key="filter_confidence", label_visibility="collapsed"
    )
    st.markdown("""
    <div style="background: rgba(0, 255, 136, 0.1); border-left: 3px solid #00ff88; padding: 0.8rem; border-radius: 8px; margin-top: 1rem;">
        <p style="color: #00ff88; margin: 0; font-size: 0.85rem;">💡 <strong>Tip:</strong> Filter set to 0.0 to show ALL issues</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div style="height: 2px; background: linear-gradient(90deg, transparent, rgba(0, 245, 255, 0.5), transparent); margin: 1.5rem 0;"></div>', unsafe_allow_html=True)

    if s3_files:
        st.markdown("""
        <div style="margin-bottom: 1rem;">
            <h3 style="color: #ffaa00; font-size: 1.3rem; font-weight: 700; text-shadow: 0 0 10px rgba(255, 170, 0, 0.5);">
                📄 AVAILABLE CSV FILES
            </h3>
        </div>
        """, unsafe_allow_html=True)
        # Only show CSV-like keys (API returns list of dicts with 'key')
        csv_files = [
            f["key"]
            for f in s3_files
            if isinstance(f, dict) and "key" in f and f["key"].lower().endswith(".csv")
        ]

        if csv_files:
            st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; font-size: 0.95rem;">🗂️ SELECT FILE</p>', unsafe_allow_html=True)
            selected_file = st.selectbox(
                "Select file from bucket:",
                options=csv_files,
                key="agentic_selected_file",
                label_visibility="collapsed"
            )

            st.markdown('<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; margin-top: 1rem; font-size: 0.95rem;">📋 FILE TYPE</p>', unsafe_allow_html=True)
            file_type = st.selectbox(
                "File type",
                options=["csv", "parquet"],
                key="agentic_file_type",
                label_visibility="collapsed"
            )

            st.markdown('<div style="margin-top: 2rem;"></div>', unsafe_allow_html=True)
            if st.button("🚀 RUN VALIDATION", key="agentic_run_validation", type="primary", use_container_width=True):
                # Clear previous results when starting new validation
                st.session_state.agentic_selected_dataset = None
                st.session_state.agentic_selected_validation_id = None
                st.session_state.agentic_latest_results = None
                st.session_state.agentic_latest_results_dataset = None
                st.session_state.agentic_latest_source_bucket = None
                st.session_state.agentic_latest_source_key = None
                st.session_state.validation_in_progress = True
                st.session_state.validating_file_name = selected_file  # Store for progress display
                st.session_state.validating_bucket = bucket  # Store bucket for validation
                st.session_state.validating_file_type = file_type  # Store file type for validation
                st.rerun()
            
            # Run validation if in progress (this will be shown on main page)
            # Note: Progress bar is shown on main page, not in sidebar
            if st.session_state.get("validation_in_progress"):
                # Don't run validation here - it will run on main page to show progress
                pass

            # Existing agentic runs section removed for now to avoid showing old results
        else:
            st.info("No CSV files found under this bucket/prefix.")
    else:
        st.info("No files found. Check bucket/prefix.")

    # Propagate selected dataset/run from session state
    selected_dataset = st.session_state.get("agentic_selected_dataset")
    selected_validation_id = st.session_state.get("agentic_selected_validation_id")

# Show progress animation FIRST if validation is running (before any other content)
if st.session_state.get("validation_in_progress"):
    validating_file = st.session_state.get("validating_file_name", st.session_state.get("agentic_selected_file", "selected file"))
    st.warning(f"⏳ Running NEW validation on **{validating_file}**. This will take 60-120 seconds...")
    
    # Horizontal progress animation on main page
    st.markdown("""
    <div class="progress-container">
        <div class="progress-header">
            <span style="font-size: 1.5rem; animation: spin 2s linear infinite;">🔄</span>
            <p class="progress-text">AI Agents are analyzing your data...</p>
        </div>
        <div class="progress-bar-container">
            <div class="progress-bar"></div>
        </div>
        <div class="progress-steps">
            <div class="progress-step active">
                <div class="progress-step-icon">📥</div>
                <div>Loading Data</div>
            </div>
            <div class="progress-step active">
                <div class="progress-step-icon">🔍</div>
                <div>Running Checks</div>
            </div>
            <div class="progress-step">
                <div class="progress-step-icon">🤖</div>
                <div>AI Analysis</div>
            </div>
            <div class="progress-step">
                <div class="progress-step-icon">✨</div>
                <div>Generating Issues</div>
            </div>
        </div>
    </div>
    <style>
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Run validation here on main page so progress is visible
    try:
        # Get file info from session state
        bucket = st.session_state.get("validating_bucket") or st.session_state.get("s3_bucket", DEFAULT_S3_BUCKET)
        selected_file = st.session_state.get("validating_file_name") or st.session_state.get("agentic_selected_file")
        file_type = st.session_state.get("validating_file_type") or st.session_state.get("agentic_file_type", "csv")
        
        if selected_file:
            config = {
                "name": selected_file,
                "source_type": "s3",
                "connection_details": {
                    "bucket": bucket,
                    "key": selected_file,
                    "file_format": file_type,
                },
                # Limit rows to keep validation fast on large files
                "max_rows": 50000,
                # Don't persist validation JSON history; generate fresh results every run
                "persist_results": False,
                "primary_key": None,
                "required_columns": [],
                "quality_checks": [
                    "null_check",
                    "duplicate_check",
                    "freshness_check",
                    "volume_check",
                ],
            }
            v_resp = requests.post(
                f"{BACKEND_URL}/api/validate", json=config, timeout=300
            )
            st.session_state.validation_in_progress = False
            if v_resp.status_code == 200:
                st.success("✅ Validation completed. Agentic issues have been computed.")
                payload = v_resp.json() or {}
                results = payload.get("results")
                if results:
                    st.session_state.agentic_latest_results = results
                    st.session_state.agentic_latest_results_dataset = selected_file
                    st.session_state.agentic_latest_source_bucket = bucket
                    st.session_state.agentic_latest_source_key = selected_file
                # For summary we use dataset name = key (same as in validation results)
                st.session_state.agentic_selected_dataset = selected_file
                st.session_state.agentic_selected_validation_id = None
                st.rerun()  # Refresh to show new results
            else:
                st.error(f"Validation failed: {v_resp.status_code} - {v_resp.text}")
    except requests.exceptions.Timeout:
        st.session_state.validation_in_progress = False
        st.error("⏱️ Validation timed out after 3 minutes. The file might be too large. Try a smaller file or reduce max_rows.")
    except Exception as e:
        st.session_state.validation_in_progress = False
        st.error(f"Error triggering validation: {e}")

# Main content - only show if we have a selected dataset (from validation or existing run)
# IMPORTANT: Results only appear AFTER clicking "Run Validation"
if selected_dataset:
    st.info(f"🔍 Loading results for: **{selected_dataset}**")
elif not st.session_state.get("agentic_selected_dataset"):
    # Always show instructional box when no dataset is selected
    st.markdown("""
    <div style="background: linear-gradient(135deg, rgba(0, 245, 255, 0.2), rgba(255, 0, 255, 0.2)); 
                border: 2px solid rgba(0, 245, 255, 0.5); border-radius: 15px; padding: 2rem; 
                text-align: center; margin: 2rem 0; box-shadow: 0 8px 32px rgba(0, 245, 255, 0.3);">
        <p style="color: #ffffff; font-size: 1.3rem; font-weight: 600; margin: 0; text-shadow: 0 0 10px rgba(255, 255, 255, 0.5);">
            👈 Select a CSV file from the sidebar and click <span style="color: #ffaa00;">'🚀 Run Validation'</span> to see agentic issues.
        </p>
    </div>
    """, unsafe_allow_html=True)

if selected_dataset:
    try:
        # Always render from the newest /api/validate response (do not read stored history)
        results = st.session_state.get("agentic_latest_results")
        results_dataset = st.session_state.get("agentic_latest_results_dataset")

        if not results or results_dataset != selected_dataset:
            st.error("No fresh validation results found in this session. Click '🚀 Run Validation' again to generate issues.")
            st.stop()

        agentic_issues = results.get("agentic_issues", []) or []
        agentic_summary = results.get("agentic_summary", {}) or {}

        # Build matrix + category counts locally
        matrix_dict = {}
        summary_by_category = {}
        for issue in agentic_issues:
            cat = issue.get("category", "N/A")
            issue_type = issue.get("issue_type", "N/A")
            summary_by_category[cat] = summary_by_category.get(cat, 0) + 1

            key = (cat, issue_type)
            if key not in matrix_dict:
                matrix_dict[key] = {
                    "category": cat,
                    "issue_type": issue_type,
                    "count": 0,
                    "dirty_example": str(issue.get("dirty_value"))[:50] if issue.get("dirty_value") is not None else "N/A",
                    "smart_fix_example": str(issue.get("suggested_value"))[:50] if issue.get("suggested_value") is not None else "N/A",
                    "why_agentic": issue.get("why_agentic") or issue.get("explanation") or "AI-Powered",
                }
            matrix_dict[key]["count"] += 1

        summary = {
            "dataset": results.get("dataset", selected_dataset),
            "total_rows_scanned": agentic_summary.get("total_rows_scanned", results.get("row_count", 0)),
            "total_issues": agentic_summary.get("total_issues", len(agentic_issues)),
            "rows_affected": agentic_summary.get("rows_affected", 0),
            "rows_affected_percent": agentic_summary.get("rows_affected_percent", 0.0),
            "summary_by_category": agentic_summary.get("category_counts", summary_by_category),
            "matrix": list(matrix_dict.values()),
        }

        # Show which dataset is being displayed
        dataset_name = summary.get("dataset", selected_dataset)

        st.markdown(f"""
        <div style="background: linear-gradient(135deg, rgba(0, 245, 255, 0.15), rgba(102, 126, 234, 0.15)); 
                    border: 1px solid rgba(0, 245, 255, 0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1rem;">
            <p style="color: #00f5ff; margin: 0; font-size: 1rem;">
                📊 <strong>Viewing Results For:</strong> <span style="color: #ffaa00; font-weight: 700;">{dataset_name}</span>
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="background: linear-gradient(135deg, rgba(102, 126, 234, 0.2), rgba(118, 75, 162, 0.2)); 
                    border: 1px solid rgba(102, 126, 234, 0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1.5rem;">
            <p style="color: #667eea; margin: 0; font-size: 1rem;">
                🤖 <strong>AI-Powered Detection:</strong> Issues from the latest validation run (not stored history)
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Summary cards
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(0, 245, 255, 0.2), rgba(0, 200, 255, 0.1)); 
                        padding: 1.5rem; border-radius: 15px; text-align: center; 
                        border: 1px solid rgba(0, 245, 255, 0.3); 
                        box-shadow: 0 8px 32px rgba(0, 245, 255, 0.2);">
                <h3 style="color: #00f5ff; margin: 0; font-size: 1rem;">📊 ROWS SCANNED</h3>
                <p style="color: white; font-size: 1.8rem; font-weight: 700; margin: 0.5rem 0 0 0;">{summary['total_rows_scanned']:,}</p>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(255, 0, 255, 0.2), rgba(200, 0, 255, 0.1)); 
                        padding: 1.5rem; border-radius: 15px; text-align: center;
                        border: 1px solid rgba(255, 0, 255, 0.3);
                        box-shadow: 0 8px 32px rgba(255, 0, 255, 0.2);">
                <h3 style="color: #ff00ff; margin: 0; font-size: 1rem;">🔍 TOTAL ISSUES</h3>
                <p style="color: white; font-size: 1.8rem; font-weight: 700; margin: 0.5rem 0 0 0;">{summary["total_issues"]}</p>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(255, 170, 0, 0.2), rgba(255, 136, 0, 0.1)); 
                        padding: 1.5rem; border-radius: 15px; text-align: center;
                        border: 1px solid rgba(255, 170, 0, 0.3);
                        box-shadow: 0 8px 32px rgba(255, 170, 0, 0.2);">
                <h3 style="color: #ffaa00; margin: 0; font-size: 1rem;">⚠️ ROWS AFFECTED</h3>
                <p style="color: white; font-size: 1.8rem; font-weight: 700; margin: 0.5rem 0 0 0;">{summary['rows_affected']:,}</p>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(0, 255, 136, 0.2), rgba(0, 200, 100, 0.1)); 
                        padding: 1.5rem; border-radius: 15px; text-align: center;
                        border: 1px solid rgba(0, 255, 136, 0.3);
                        box-shadow: 0 8px 32px rgba(0, 255, 136, 0.2);">
                <h3 style="color: #00ff88; margin: 0; font-size: 1rem;">📈 AFFECTED %</h3>
                <p style="color: white; font-size: 1.8rem; font-weight: 700; margin: 0.5rem 0 0 0;">{summary['rows_affected_percent']:.1f}%</p>
            </div>
            """, unsafe_allow_html=True)

        # Category breakdown with modern heading
        st.markdown('<div style="margin-top: 3rem;"></div>', unsafe_allow_html=True)
        st.markdown("""
        <h2 style="text-align: center; color: #00f5ff; font-size: 2rem; font-weight: 700; 
                   text-shadow: 0 0 20px rgba(0, 245, 255, 0.5);">
            📊 ISSUES BY CATEGORY
        </h2>
        """, unsafe_allow_html=True)
        st.markdown('<div style="margin-bottom: 2rem;"></div>', unsafe_allow_html=True)
        category_counts = summary.get("summary_by_category", {})
        if category_counts:
            cat_df = pd.DataFrame(list(category_counts.items()), columns=["Category", "Count"])
            st.bar_chart(cat_df.set_index("Category"))

        st.markdown("---")

        # Show all issues directly - no matrix needed
        if agentic_issues:
            # Get all unique issue types for display
            all_issue_types = list(set([issue["issue_type"] for issue in agentic_issues]))
            
            # Show all issues by default
            selected_issue_types = all_issue_types
            
            # Use detailed issues from newest validation run (no stored history)
            issues = agentic_issues

            # Filter by selected issue types and confidence
            filtered_issues = []
            for issue in issues:
                if issue["issue_type"] in selected_issue_types:
                    if issue["confidence"] >= filter_confidence:
                        filtered_issues.append(issue)

            if filtered_issues:
                # Unit preferences section (for ScaleMismatch issues) - show BEFORE issue table
                unit_issue_columns = set()
                for issue in filtered_issues:
                    if issue.get('issue_type') == 'ScaleMismatch':
                        unit_issue_columns.add(issue.get('column'))
            
                unit_preferences = {}
                if unit_issue_columns:
                    st.markdown("""
                    <div style="margin-top: 2rem; margin-bottom: 1rem;">
                        <h2 style="text-align: center; color: #667eea; font-size: 1.8rem; font-weight: 700; 
                                   text-shadow: 0 0 20px rgba(102, 126, 234, 0.5);">
                            ⚙️ UNIT PREFERENCES
                        </h2>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown("""
                    <div style="background: linear-gradient(135deg, rgba(0, 245, 255, 0.1), rgba(102, 126, 234, 0.1)); 
                                border: 1px solid rgba(0, 245, 255, 0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1.5rem;">
                        <p style="color: #00f5ff; margin: 0; font-size: 0.95rem;">
                            📏 Select your preferred unit for each column. All values will be standardized to this unit automatically.
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                
                    for col in sorted(unit_issue_columns):
                        col_lower = col.lower()
                        # Determine default unit based on column name
                        if 'weight' in col_lower:
                            default_unit = 'kg'
                            options = ['kg', 'g', 'lb', 'oz']
                        elif 'height' in col_lower or 'length' in col_lower or 'distance' in col_lower:
                            default_unit = 'cm'
                            options = ['cm', 'm', 'in', 'ft']
                        else:
                            default_unit = 'cm'
                            options = ['cm', 'm', 'in', 'ft', 'kg', 'g']
                    
                        # Get or set preference
                        pref_key = f"unit_pref_{col}"
                        if pref_key not in st.session_state:
                            st.session_state[pref_key] = default_unit
                    
                        st.markdown(f'<p style="color: #00f5ff; font-weight: 600; margin-bottom: 0.5rem; font-size: 0.9rem;">📏 UNIT FOR \'{col}\'</p>', unsafe_allow_html=True)
                        selected_unit = st.selectbox(
                            f"Unit for '{col}':",
                            options=options,
                            index=options.index(st.session_state[pref_key]) if st.session_state[pref_key] in options else 0,
                            key=pref_key,
                            label_visibility="collapsed"
                        )
                        unit_preferences[col] = selected_unit
                
                    st.session_state.unit_preferences = unit_preferences
            
                # Initialize selected issues in session state
                if 'selected_issue_ids' not in st.session_state:
                    st.session_state.selected_issue_ids = set()
                
                # Get ALL issue IDs from filtered issues
                all_issue_ids = [i.get('id') for i in filtered_issues if i.get('id')]
                
                # Create DataFrame for table display with select column
                st.markdown("""
                <div style="margin-top: 2rem; margin-bottom: 1.5rem;">
                    <h2 style="text-align: center; color: #ff00ff; font-size: 2rem; font-weight: 700; 
                               text-shadow: 0 0 20px rgba(255, 0, 255, 0.5);">
                        📋 ISSUE DETAILS
                    </h2>
                    <p style="text-align: center; color: #b0b0b0; font-size: 1rem; margin-top: 0.5rem;">
                        Select issues to apply smart fixes
                    </p>
                </div>
                """, unsafe_allow_html=True)
                
                # "Select All" checkbox and count
                col_select_all, col_count = st.columns([3, 1])
                with col_select_all:
                    currently_selected = st.session_state.selected_issue_ids
                    all_visible_selected = len(all_issue_ids) > 0 and all(issue_id in currently_selected for issue_id in all_issue_ids if issue_id)
                    
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(0, 245, 255, 0.1), rgba(255, 0, 255, 0.1)); 
                                border: 1px solid rgba(0, 245, 255, 0.3); border-radius: 10px; padding: 0.8rem; margin-bottom: 1rem;">
                        <p style="color: #00f5ff; margin: 0; font-weight: 600; font-size: 1rem;">
                            ☑️ Select All ({len(all_issue_ids)} issues)
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                    select_all = st.checkbox(
                        f"Select All ({len(all_issue_ids)} issues)",
                        value=all_visible_selected,
                        key="select_all_issues",
                        label_visibility="collapsed"
                    )
                    
                    if select_all != all_visible_selected:
                        if select_all:
                            st.session_state.selected_issue_ids.update(all_issue_ids)
                        else:
                            for issue_id in all_issue_ids:
                                st.session_state.selected_issue_ids.discard(issue_id)
                        st.rerun()
                
                with col_count:
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(0, 255, 136, 0.2), rgba(0, 200, 100, 0.1)); 
                                padding: 1rem; border-radius: 10px; text-align: center;
                                border: 1px solid rgba(0, 255, 136, 0.3);">
                        <h4 style="color: #00ff88; margin: 0; font-size: 0.9rem;">✅ SELECTED</h4>
                        <p style="color: white; font-size: 1.5rem; font-weight: 700; margin: 0.3rem 0 0 0;">{len(st.session_state.selected_issue_ids)}</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Build table data
                table_data = []
                issue_id_map = {}  # Map index to issue_id
                        
                for idx, issue in enumerate(filtered_issues):
                    issue_id = issue.get('id')
                    issue_id_map[idx] = issue_id
                    suggested_value = str(issue['suggested_value'])
                    
                    # If this is a unit issue and we have a preference, recalculate
                    if issue.get('issue_type') == 'ScaleMismatch' and issue.get('column') in unit_preferences:
                        preferred_unit = unit_preferences[issue.get('column')]
                        dirty_value = str(issue.get('dirty_value', ''))
                        
                        try:
                            import re
                            match = re.search(r'([\d.]+)\s*(\w+)', dirty_value)
                            if match:
                                numeric_val = float(match.group(1))
                                current_unit = match.group(2).lower()
                                
                                unit_map = {
                                    'cm': 'cm', 'm': 'm', 'in': 'in', 'ft': 'ft',
                                    'kg': 'kg', 'g': 'g', 'lb': 'lb', 'oz': 'oz'
                                }
                                current_unit_normalized = unit_map.get(current_unit, current_unit)
                                
                                if current_unit_normalized != preferred_unit:
                                    converted = convert_units_frontend(numeric_val, current_unit_normalized, preferred_unit)
                                    if converted is not None:
                                        suggested_value = f"{converted:.2f} {preferred_unit}"
                        except:
                            pass
                    
                    is_selected = issue_id in st.session_state.selected_issue_ids
                    
                    table_data.append({
                        "✓": is_selected,
                        "Row": issue.get('row_id', 'N/A'),
                        "Column": issue['column'],
                        "Issue Type": issue['issue_type'],
                        "Current Value": str(issue['dirty_value'])[:40],
                        "Suggested Fix": suggested_value[:40],
                        "Confidence": round(issue['confidence'], 2),
                        "Explanation": issue.get('explanation', '')[:80]
                    })
                
                # Display table with selection column
                if table_data:
                    issues_df = pd.DataFrame(table_data)
                    
                    # Use data_editor with checkbox column
                    edited_df = st.data_editor(
                        issues_df,
                        use_container_width=True,
                        hide_index=True,
                        disabled=["Row", "Column", "Issue Type", "Current Value", "Suggested Fix", "Confidence", "Explanation"],
                        column_config={
                            "✓": st.column_config.CheckboxColumn(
                                "✓",
                                help="Select to fix",
                                default=False,
                            )
                        },
                        key="issues_table"
                    )
                    
                    # Update selected issues based on edited dataframe
                    new_selected = set()
                    for idx, row in edited_df.iterrows():
                        if row["✓"] == True:
                            if idx in issue_id_map:
                                new_selected.add(issue_id_map[idx])
                    
                    if new_selected != st.session_state.selected_issue_ids:
                        st.session_state.selected_issue_ids = new_selected
                        st.rerun()
                
                st.markdown('<div style="height: 2px; background: linear-gradient(90deg, transparent, rgba(255, 0, 255, 0.5), transparent); margin: 2rem 0;"></div>', unsafe_allow_html=True)
                    
                # Action buttons: preview and apply fixes
                st.markdown("""
                <div style="margin-bottom: 1.5rem;">
                    <h2 style="text-align: center; color: #ffaa00; font-size: 2rem; font-weight: 700; 
                               text-shadow: 0 0 20px rgba(255, 170, 0, 0.5);">
                        🔧 APPLY FIXES
                    </h2>
                </div>
                """, unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                    
                with col1:
                    if st.button("👁️ Preview Cleaned CSV", key="preview_fixes_btn"):
                        try:
                            # Only use selected issues
                            selected_ids = list(st.session_state.selected_issue_ids)
                            if not selected_ids:
                                st.warning("⚠️ Please select at least one issue to fix by checking the checkboxes above.")
                            else:
                                # Filter to only selected issues
                                selected_issues = [i for i in filtered_issues if i.get('id') in selected_ids]
                                with st.spinner(f"Generating preview for {len(selected_ids)} selected issues..."):
                                    payload = {
                                        "issue_ids": selected_ids,
                                        "mode": "preview",
                                        # Provide issues + source directly (no stored validation JSON required)
                                        "issues": selected_issues,
                                        "source_bucket": st.session_state.get("agentic_latest_source_bucket"),
                                        "source_key": st.session_state.get("agentic_latest_source_key"),
                                        "unit_preferences": st.session_state.get("unit_preferences", {}),
                                    }
                                    apply_resp = requests.post(
                                        f"{BACKEND_URL}/api/agents/apply",
                                        json=payload,
                                        timeout=60,
                                    )
                                    if apply_resp.status_code == 200:
                                        apply_data = apply_resp.json()
                                        preview_data = apply_data.get("preview_data", {})
                                        csv_base64 = preview_data.get("csv_base64")
                                        filename = preview_data.get("filename", "cleaned.csv")
                                        applied_count = preview_data.get("applied_count", 0)
                                    
                                        if csv_base64:
                                            st.success(f"✅ Preview ready: {applied_count} fixes applied by AI")
                                            st.session_state.cleaned_csv_base64 = csv_base64
                                            st.session_state.cleaned_csv_filename = filename
                                            st.session_state.applied_details = preview_data.get("applied_details", [])
                                            st.session_state.changed_cells = preview_data.get("changed_cells", {}) or {}
                                            st.session_state.csv_original_base64 = preview_data.get("csv_original_base64")
                                            st.rerun()  # Rerun to show preview
                                        else:
                                            st.error("Preview data not available. Check backend logs for errors.")
                                    else:
                                        st.error(f"Preview failed: {apply_resp.status_code} - {apply_resp.text}")
                        except Exception as e:
                            st.error(f"Error generating preview: {e}")
                    
                with col2:
                    if st.session_state.get("cleaned_csv_base64"):
                        import base64
                        csv_base64 = st.session_state.get("cleaned_csv_base64")
                        filename = st.session_state.get("cleaned_csv_filename", "cleaned.csv")
                        csv_bytes = base64.b64decode(csv_base64)
                        st.download_button(
                            label="📥 Download Cleaned CSV",
                            data=csv_bytes,
                            file_name=filename,
                            mime="text/csv",
                            key="download_cleaned_csv"
                        )
                    
                with col3:
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(255, 0, 255, 0.2), rgba(200, 0, 255, 0.1)); 
                                padding: 1rem; border-radius: 10px; text-align: center;
                                border: 1px solid rgba(255, 0, 255, 0.3);">
                        <h4 style="color: #ff00ff; margin: 0; font-size: 0.9rem;">📊 SELECTED</h4>
                        <p style="color: white; font-size: 1.5rem; font-weight: 700; margin: 0.3rem 0 0 0;">{len(st.session_state.selected_issue_ids)}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                # Show full CSV preview with green highlighting for changed values
                if st.session_state.get("cleaned_csv_base64"):
                    st.markdown("""
                    <div style="margin-top: 2rem; margin-bottom: 1.5rem;">
                        <h2 style="text-align: center; color: #00ff88; font-size: 2rem; font-weight: 700; 
                                   text-shadow: 0 0 20px rgba(0, 255, 136, 0.5);">
                            📊 PREVIEW: CLEANED CSV
                        </h2>
                        <p style="text-align: center; color: #b0b0b0; font-size: 1rem; margin-top: 0.5rem;">
                            Changed values highlighted in green
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                
                    # Show AI indicator
                    st.markdown("""
                    <div style="background: linear-gradient(135deg, rgba(102, 126, 234, 0.2), rgba(118, 75, 162, 0.2)); 
                                border: 1px solid rgba(102, 126, 234, 0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1.5rem;">
                        <p style="color: #667eea; margin: 0; font-size: 1rem;">
                            🤖 <strong>AI-Powered Fixes:</strong> All corrections made by AI agents based on your data patterns (not hardcoded)
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                
                    import base64
                    from io import StringIO
                
                    # Decode cleaned CSV
                    csv_base64 = st.session_state.get("cleaned_csv_base64")
                    if csv_base64:
                        try:
                            csv_bytes = base64.b64decode(csv_base64)
                            csv_string = csv_bytes.decode("utf-8")
                            df_preview = pd.read_csv(StringIO(csv_string))
                        
                            # Get changed cells mapping
                            changed_cells = st.session_state.get("changed_cells", {})
                        
                            # Create a DataFrame of styles
                            styles = pd.DataFrame('', index=df_preview.index, columns=df_preview.columns)
                            for key, change_info in changed_cells.items():
                                try:
                                    # Handle both string keys and dict values
                                    if isinstance(key, str) and '_' in key:
                                        row_idx_str, col_name = key.split('_', 1)
                                        row_idx = int(row_idx_str)
                                        if row_idx < len(df_preview) and col_name in df_preview.columns:
                                            styles.at[row_idx, col_name] = 'background-color: #90EE90'
                                except (ValueError, KeyError, AttributeError) as e:
                                    continue
                        
                            # Apply styles
                            styled_df = df_preview.style.apply(lambda x: styles, axis=None)
                        
                            # Display styled dataframe
                            st.dataframe(styled_df, use_container_width=True, height=400)
                        
                            # Show legend
                            st.caption("🟢 Green cells indicate values that were changed/fixed by AI")
                        except Exception as e:
                            st.error(f"Error displaying preview: {e}")
                            st.code(str(e))
                    else:
                        st.warning("Preview data not available")
                    
                # Show applied fixes details if preview was generated
                if st.session_state.get("applied_details"):
                    with st.expander("📋 View Applied Fixes Details"):
                        applied_df = pd.DataFrame(st.session_state.get("applied_details", []))
                        st.dataframe(applied_df, use_container_width=True, hide_index=True)
                    
                # Option to save to S3 after preview
                if st.session_state.get("cleaned_csv_base64"):
                    if st.button("💾 Save Cleaned CSV to S3", key="save_to_s3_btn"):
                        try:
                            issue_ids = [i['id'] for i in filtered_issues if i.get('id')]
                            payload = {
                                "issue_ids": issue_ids,
                                "mode": "export",
                                # Provide issues + source directly (no stored validation JSON required)
                                "issues": filtered_issues,
                                "source_bucket": st.session_state.get("agentic_latest_source_bucket"),
                                "source_key": st.session_state.get("agentic_latest_source_key"),
                                "unit_preferences": st.session_state.get("unit_preferences", {}),
                            }
                            with st.spinner("Saving to S3..."):
                                apply_resp = requests.post(
                                    f"{BACKEND_URL}/api/agents/apply",
                                    json=payload,
                                    timeout=60,
                                )
                                if apply_resp.status_code == 200:
                                    apply_data = apply_resp.json()
                                    st.success(apply_data.get("message", "Cleaned CSV saved to S3"))
                                    download_url = apply_data.get("download_url")
                                    if download_url:
                                        st.code(download_url, language="text")
                                else:
                                    st.error(f"Save failed: {apply_resp.status_code} - {apply_resp.text}")
                        except Exception as e:
                            st.error(f"Error saving to S3: {e}")
                    else:
                                st.info("No issues match the selected filters.")
                else:
                    st.info("No issues found for the selected filters.")
        else:
            st.info("No agentic issues found for this dataset.")
    except Exception as e:
        st.error(f"Error: {e}")