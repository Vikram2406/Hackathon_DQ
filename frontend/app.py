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

# Ensure white/light theme
st.markdown("""
<style>
    /* Light/White Theme */
    .stApp {
        background-color: #ffffff !important;
        color: #262730 !important;
    }
    
    .main .block-container {
        background-color: #ffffff !important;
        color: #262730 !important;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #f0f2f6 !important;
    }
    
    /* Text colors */
    h1, h2, h3, h4, h5, h6, p, div, span {
        color: #262730 !important;
    }
    
    /* Input fields */
    .stTextInput>div>div>input,
    .stSelectbox>div>div {
        background-color: #ffffff !important;
        color: #262730 !important;
    }
    
    /* Buttons */
    .stButton>button {
        background-color: #1f77b4 !important;
        color: #ffffff !important;
    }
    
    /* Hide navigation if no pages */
    [data-testid="stSidebarNav"] {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# Simple black and white theme - standard Streamlit styling
st.title("🤖 Agentic DQ Platform")
st.markdown("AI-Powered Smart Data Fixes • Real-time Issue Detection • Zero Code Required")

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
    st.header("📁 S3 File Browser")
    st.caption("Select your data source")

    # Remember selections across reruns
    if "agentic_selected_dataset" not in st.session_state:
        st.session_state.agentic_selected_dataset = None
    if "agentic_selected_validation_id" not in st.session_state:
        st.session_state.agentic_selected_validation_id = None

    bucket = st.text_input("🪣 S3 Bucket", value=DEFAULT_S3_BUCKET, key="s3_bucket")
    prefix = st.text_input("📂 Prefix (optional)", value="", key="s3_prefix")

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
    st.subheader("🎛️ Filters")
    
    filter_category = st.selectbox(
        "📁 Category",
        ["All", "Semantic", "Logic", "Formatting", "Imputation", "Extraction", "Categorical", "Units"],
        key="filter_category"
    )
    
    filter_confidence = st.slider(
        "📊 Min Confidence", 0.0, 1.0, 0.0, 0.1, key="filter_confidence"
    )
    st.info("💡 Tip: Filter set to 0.0 to show ALL issues")

    if s3_files:
        st.subheader("📄 Available CSV Files")
        # Only show CSV-like keys (API returns list of dicts with 'key')
        csv_files = [
            f["key"]
            for f in s3_files
            if isinstance(f, dict) and "key" in f and f["key"].lower().endswith(".csv")
        ]

        if csv_files:
            selected_file = st.selectbox(
                "🗂️ Select File",
                options=csv_files,
                key="agentic_selected_file"
            )

            file_type = st.selectbox(
                "📋 File Type",
                options=["csv", "parquet"],
                key="agentic_file_type"
            )
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
    
    # Simple progress indicator
    with st.spinner("🔄 AI Agents are analyzing your data..."):
        pass
    
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
    st.info("👈 Select a CSV file from the sidebar and click '🚀 Run Validation' to see agentic issues.")

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

        st.info(f"📊 Viewing Results For: **{dataset_name}**")
        st.info("🤖 AI-Powered Detection: Issues from the latest validation run (not stored history)")

        # Summary cards
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("📊 Rows Scanned", f"{summary['total_rows_scanned']:,}")
        with col2:
            st.metric("🔍 Total Issues", summary["total_issues"])
        with col3:
            st.metric("⚠️ Rows Affected", f"{summary['rows_affected']:,}")
        with col4:
            st.metric("📈 Affected %", f"{summary['rows_affected_percent']:.1f}%")

        # Category breakdown
        st.subheader("📊 Issues by Category")
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
                    st.subheader("⚙️ Unit Preferences")
                    
                    st.info("📏 Select your preferred unit for each column. All values will be standardized to this unit automatically.")
                
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
                    
                        selected_unit = st.selectbox(
                            f"📏 Unit for '{col}'",
                            options=options,
                            index=options.index(st.session_state[pref_key]) if st.session_state[pref_key] in options else 0,
                            key=pref_key
                        )
                        unit_preferences[col] = selected_unit
                
                    st.session_state.unit_preferences = unit_preferences
            
                # Initialize selected issues in session state
                if 'selected_issue_ids' not in st.session_state:
                    st.session_state.selected_issue_ids = set()
                
                # Get ALL issue IDs from filtered issues
                all_issue_ids = [i.get('id') for i in filtered_issues if i.get('id')]
                
                # Create DataFrame for table display with select column
                st.subheader("📋 Issue Details")
                st.caption("Select issues to apply smart fixes")
                
                # "Select All" checkbox and count
                col_select_all, col_count = st.columns([3, 1])
                with col_select_all:
                    currently_selected = st.session_state.selected_issue_ids
                    all_visible_selected = len(all_issue_ids) > 0 and all(issue_id in currently_selected for issue_id in all_issue_ids if issue_id)
                    
                    select_all = st.checkbox(
                        f"☑️ Select All ({len(all_issue_ids)} issues)",
                        value=all_visible_selected,
                        key="select_all_issues"
                    )
                    
                    if select_all != all_visible_selected:
                        if select_all:
                            st.session_state.selected_issue_ids.update(all_issue_ids)
                        else:
                            for issue_id in all_issue_ids:
                                st.session_state.selected_issue_ids.discard(issue_id)
                        st.rerun()
                
                with col_count:
                    st.metric("✅ Selected", len(st.session_state.selected_issue_ids))
                
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
                
                st.markdown("---")
                    
                # Action buttons: preview and apply fixes
                st.subheader("🔧 Apply Fixes")
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
                    st.metric("📊 Selected", len(st.session_state.selected_issue_ids))
                    
                # Show full CSV preview with green highlighting for changed values
                if st.session_state.get("cleaned_csv_base64"):
                    st.subheader("📊 Preview: Cleaned CSV")
                    st.caption("Changed values highlighted in green")
                    st.info("🤖 AI-Powered Fixes: All corrections made by AI agents based on your data patterns (not hardcoded)")
                
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