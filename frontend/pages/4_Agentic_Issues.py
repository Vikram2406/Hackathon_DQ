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

st.set_page_config(page_title="Agentic Issues", page_icon="🤖", layout="wide")

st.title("🤖 Agentic Data Quality Issues")
st.markdown("AI-powered data quality issue detection and smart fixes on top of your S3 CSV files.")

# Store the latest validation results locally (no history / no caching)
if "agentic_latest_results" not in st.session_state:
    st.session_state.agentic_latest_results = None
if "agentic_latest_results_dataset" not in st.session_state:
    st.session_state.agentic_latest_results_dataset = None
if "agentic_latest_source_bucket" not in st.session_state:
    st.session_state.agentic_latest_source_bucket = None
if "agentic_latest_source_key" not in st.session_state:
    st.session_state.agentic_latest_source_key = None

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

    # Remember selections across reruns
    if "agentic_selected_dataset" not in st.session_state:
        st.session_state.agentic_selected_dataset = None
    if "agentic_selected_validation_id" not in st.session_state:
        st.session_state.agentic_selected_validation_id = None

    bucket = st.text_input("S3 Bucket", value=DEFAULT_S3_BUCKET, key="s3_bucket")
    prefix = st.text_input("Prefix (optional)", value="", key="s3_prefix")

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
    st.markdown("### Filters")
    filter_category = st.selectbox(
        "Category:",
        ["All", "Semantic", "Logic", "Formatting", "Imputation", "Extraction", "Categorical", "Units"],
        key="filter_category",
    )
    filter_confidence = st.slider(
        "Min Confidence:", 0.0, 1.0, 0.0, 0.1, key="filter_confidence"
    )
    st.info("💡 Tip: Confidence filter is set to 0.0 to show ALL issues. Increase to filter low-confidence issues.")

    st.markdown("---")

    if s3_files:
        st.markdown("### Available CSV Files")
        # Only show CSV-like keys (API returns list of dicts with 'key')
        csv_files = [
            f["key"]
            for f in s3_files
            if isinstance(f, dict) and "key" in f and f["key"].lower().endswith(".csv")
        ]

        if csv_files:
            selected_file = st.selectbox(
                "Select file from bucket:",
                options=csv_files,
                key="agentic_selected_file",
            )

            file_type = st.selectbox(
                "File type",
                options=["csv", "parquet"],
                key="agentic_file_type",
            )

            if st.button("🚀 Run Validation on Selected File", key="agentic_run_validation", type="primary"):
                # Clear previous results when starting new validation
                st.session_state.agentic_selected_dataset = None
                st.session_state.agentic_selected_validation_id = None
                st.session_state.agentic_latest_results = None
                st.session_state.agentic_latest_results_dataset = None
                st.session_state.agentic_latest_source_bucket = None
                st.session_state.agentic_latest_source_key = None
                
                st.warning(f"⏳ Running NEW validation on **{selected_file}**. This will take 60-120 seconds...")
                
                with st.spinner("🔄 Running validation and agentic analysis..."):
                    try:
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
                        st.error("⏱️ Validation timed out after 3 minutes. The file might be too large. Try a smaller file or reduce max_rows.")
                    except Exception as e:
                        st.error(f"Error triggering validation: {e}")

            # Existing agentic runs section removed for now to avoid showing old results
        else:
            st.info("No CSV files found under this bucket/prefix.")
    else:
        st.info("No files found. Check bucket/prefix.")

    # Propagate selected dataset/run from session state
    selected_dataset = st.session_state.get("agentic_selected_dataset")
    selected_validation_id = st.session_state.get("agentic_selected_validation_id")

# Main content - only show if we have a selected dataset (from validation or existing run)
# IMPORTANT: Results only appear AFTER clicking "Run Validation"
if selected_dataset:
    st.info(f"🔍 Loading results for: **{selected_dataset}**")
elif not st.session_state.get("agentic_selected_dataset"):
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

        st.info(f"📊 **Viewing Results For**: `{dataset_name}`")
        st.info("🤖 **AI-Powered Detection**: Issues below are from the newest validation run (not stored history).")

        # Summary cards
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Rows Scanned", f"{summary['total_rows_scanned']:,}")
        with col2:
            st.metric("Total Issues", summary["total_issues"])
        with col3:
            st.metric("Rows Affected", f"{summary['rows_affected']:,}")
        with col4:
            st.metric("Affected %", f"{summary['rows_affected_percent']:.1f}%")

        # Category breakdown
        st.markdown("### 📊 Issues by Category")
        category_counts = summary.get("summary_by_category", {})
        if category_counts:
            cat_df = pd.DataFrame(list(category_counts.items()), columns=["Category", "Count"])
            st.bar_chart(cat_df.set_index("Category"))

        st.markdown("---")

        # Issue Matrix
        st.markdown("### 📋 Issue Matrix")
        matrix = summary.get("matrix", [])

        if matrix:
            # Create DataFrame for matrix
            matrix_data = []
            for item in matrix:
                matrix_data.append(
                    {
                        "Category": item["category"],
                        "Issue Type": item["issue_type"],
                        "Dirty Example": item.get("dirty_example", "N/A"),
                        "Agent's Smart Fix": item.get("smart_fix_example", "N/A"),
                        "Why it's Agentic": item.get("why_agentic", "N/A"),
                        "Count": item["count"],
                    }
                )

            matrix_df = pd.DataFrame(matrix_data)

            # Apply category filter
            if filter_category != "All":
                matrix_df = matrix_df[matrix_df["Category"] == filter_category]

            # Display matrix
            st.dataframe(
                matrix_df,
                use_container_width=True,
                hide_index=True,
            )

            # Drill-down: Select issue type
            st.markdown("---")
            st.markdown("### 🔍 Issue Details")

            if len(matrix_df) > 0:
                # Auto-select ALL issue types by default to show all issues
                all_issue_types = matrix_df["Issue Type"].unique().tolist()
                selected_issue_types = st.multiselect(
                    "Select Issue Types to View Details:",
                    options=all_issue_types,
                    default=all_issue_types,  # Auto-select ALL by default
                    key="selected_issue_types",
                )

                if selected_issue_types:
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
                            st.markdown("### ⚙️ Unit Preferences")
                            st.info("Select your preferred unit for each column. All values will be standardized to this unit. The table below will update automatically.")
                        
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
                                    f"Unit for '{col}':",
                                    options=options,
                                    index=options.index(st.session_state[pref_key]) if st.session_state[pref_key] in options else 0,
                                    key=pref_key
                                )
                                unit_preferences[col] = selected_unit
                        
                            st.session_state.unit_preferences = unit_preferences
                    
                        # Initialize selected issues in session state
                        if 'selected_issue_ids' not in st.session_state:
                            st.session_state.selected_issue_ids = set()
                        
                        # Initialize selection version counter (increments when Select All is toggled)
                        if 'selection_version' not in st.session_state:
                            st.session_state.selection_version = 0
                        
                        # Get ALL issue IDs from filtered issues (these are the ones currently visible)
                        all_issue_ids = [i.get('id') for i in filtered_issues if i.get('id')]
                        
                        # "Select All" checkbox
                        st.markdown("### 📋 Issue Details (Select issues to fix)")
                        col_select_all, col_count = st.columns([3, 1])
                        with col_select_all:
                            # Check current state - are all visible issues selected?
                            currently_selected = st.session_state.selected_issue_ids
                            all_visible_selected = len(all_issue_ids) > 0 and all(issue_id in currently_selected for issue_id in all_issue_ids if issue_id)
                            
                            select_all = st.checkbox(
                                f"Select All ({len(all_issue_ids)} issues)",
                                value=all_visible_selected,
                                key="select_all_issues"
                            )
                            
                            # Handle selection/deselection based on checkbox state change
                            if select_all != all_visible_selected:
                                if select_all:
                                    # User checked "Select All" - add all visible issues
                                    st.session_state.selected_issue_ids.update(all_issue_ids)
                                else:
                                    # User unchecked "Select All" - remove all visible issues
                                    for issue_id in all_issue_ids:
                                        st.session_state.selected_issue_ids.discard(issue_id)
                                # Increment version to force checkbox recreation
                                st.session_state.selection_version += 1
                                # Force a rerun so individual checkboxes reflect the new state
                                st.rerun()
                        
                        with col_count:
                            st.metric("Selected", len(st.session_state.selected_issue_ids))
                        
                        # Create DataFrame for display with checkboxes - recalculate suggested values based on unit preferences
                        issues_df_data = []
                        for idx, issue in enumerate(filtered_issues):
                            issue_id = issue.get('id')
                            suggested_value = str(issue['suggested_value'])[:50]
                        
                            # If this is a unit issue and we have a preference, recalculate
                            if issue.get('issue_type') == 'ScaleMismatch' and issue.get('column') in unit_preferences:
                                preferred_unit = unit_preferences[issue.get('column')]
                                dirty_value = str(issue.get('dirty_value', ''))
                            
                                # Try to parse and convert to preferred unit
                                try:
                                    import re
                                    # Extract number and unit from dirty value
                                    # Handle formats like "5ft 10in", "178cm", "1.78 meters"
                                    match = re.search(r'([\d.]+)\s*(\w+)', dirty_value)
                                    if match:
                                        numeric_val = float(match.group(1))
                                        current_unit = match.group(2).lower()
                                    
                                        # Map common unit abbreviations to standard names
                                        unit_map = {
                                            'cm': 'cm', 'centimeter': 'cm', 'centimeters': 'cm',
                                            'm': 'm', 'meter': 'm', 'meters': 'm', 'metre': 'm', 'metres': 'm',
                                            'in': 'in', 'inch': 'in', 'inches': 'in', '"': 'in',
                                            'ft': 'ft', 'feet': 'ft', 'foot': 'ft', "'": 'ft',
                                            'kg': 'kg', 'kilogram': 'kg', 'kilograms': 'kg',
                                            'g': 'g', 'gram': 'g', 'grams': 'g',
                                            'lb': 'lb', 'pound': 'lb', 'pounds': 'lb', 'lbs': 'lb',
                                            'oz': 'oz', 'ounce': 'oz', 'ounces': 'oz'
                                        }
                                    
                                        current_unit_normalized = unit_map.get(current_unit, current_unit)
                                    
                                        # Convert if units are different
                                        if current_unit_normalized != preferred_unit:
                                            converted = convert_units_frontend(numeric_val, current_unit_normalized, preferred_unit)
                                            if converted is not None:
                                                suggested_value = f"{converted:.2f} {preferred_unit}"
                                except Exception as e:
                                    pass  # Keep original suggested value if conversion fails
                            
                            # Create checkbox for each issue
                            checkbox_col, info_col = st.columns([0.3, 9.7])
                            with checkbox_col:
                                # Check if this issue should be selected (either individually or via Select All)
                                should_be_selected = issue_id in st.session_state.selected_issue_ids
                                
                                # Include selection_version in key to force recreation when Select All is toggled
                                checkbox_key = f"issue_checkbox_{issue_id}_{st.session_state.selection_version}"
                                
                                is_selected = st.checkbox(
                                    "",
                                    value=should_be_selected,
                                    key=checkbox_key,
                                    label_visibility="collapsed"
                                )
                                
                                # Update session state based on checkbox state
                                if is_selected and issue_id and issue_id not in st.session_state.selected_issue_ids:
                                    st.session_state.selected_issue_ids.add(issue_id)
                                    # Rerun to update Select All checkbox state (but don't increment version)
                                    st.rerun()
                                elif not is_selected and issue_id and issue_id in st.session_state.selected_issue_ids:
                                    st.session_state.selected_issue_ids.discard(issue_id)
                                    # Rerun to update Select All checkbox state (but don't increment version)
                                    st.rerun()
                            
                            with info_col:
                                # Display issue details in a compact format
                                issue_text = f"**Row {issue.get('row_id', 'N/A')}** | Column: `{issue['column']}` | {issue['dirty_value']} → **{suggested_value}** | Confidence: {issue['confidence']:.2f}"
                                if issue.get('explanation'):
                                    issue_text += f" | _{issue['explanation'][:80]}_"
                                st.markdown(issue_text)
                        
                        st.markdown("---")
                    
                        # Action buttons: preview and apply fixes
                        st.markdown("### 🔧 Apply Fixes")
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
                            st.metric("Selected Issues", len(st.session_state.selected_issue_ids))
                    
                        # Show full CSV preview with green highlighting for changed values
                        if st.session_state.get("cleaned_csv_base64"):
                            st.markdown("### 📊 Preview: Cleaned CSV (Changed values highlighted in green)")
                        
                            # Show AI indicator
                            st.info("🤖 **AI-Powered Fixes**: All corrections were made by AI agents based on your data patterns (not hardcoded).")
                        
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
                        st.info("Select one or more issue types from the matrix above to view details.")
                else:
                    st.info("No issues found for the selected filters.")
            else:
                st.info("No agentic issues found for this dataset.")
    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("👈 Please select a dataset from the sidebar to view agentic issues.")