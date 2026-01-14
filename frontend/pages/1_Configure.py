"""
Enhanced Configure Page - Detailed Results Like CLI
"""
import streamlit as st
import requests
import os
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
AWS_REGIONS = [
    "us-east-1", "us-west-2", "eu-west-1", "ap-south-1", 
    "ap-southeast-1", "ap-northeast-1"
]

st.set_page_config(page_title="Configure", page_icon="⚙️", layout="wide")

st.title("⚙️ Configure Data Quality Checks")
st.markdown("Set up your data source and quality validation rules")

# Step 1: Source Selection
st.markdown("## 1️⃣ Select Data Source")

# Only show implemented sources
st.info("💡 **Currently Supported:** AWS S3 only. Databricks and Snowflake coming soon!")

source_type = st.selectbox(
    "Choose your data source:",
    options=["s3"],  # Only S3 is implemented
    format_func=lambda x: {
        "s3": "☁️ AWS S3 (Available)",
    }[x],
    key="source_type"
)

# Future sources preview (disabled)
with st.expander("🔮 Coming Soon"):
    st.markdown("""
    **Future Data Sources:**
    - 🧱 Databricks (In Development)
    - ❄️ Snowflake (In Development)
    - 📄 Local CSV (Available in validation script)
    
    These will be added in future releases.
    """)

st.markdown("---")

# Step 2: Hierarchical Configuration based on source
st.markdown("## 2️⃣ Configure Connection")

connection_details = {}
dataset_name = None
primary_key = None

if source_type == "s3":
    st.markdown("### AWS S3 Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Step 2.1: Select Region
        region = st.selectbox(
            "1. Select AWS Region:",
            options=AWS_REGIONS,
            index=AWS_REGIONS.index("ap-south-1") if "ap-south-1" in AWS_REGIONS else 0,
            help="Choose the AWS region where your bucket is located"
        )
    
    with col2:
        # Step 2.2: Enter Bucket Name
        bucket = st.text_input(
            "2. Enter Bucket Name:",
            value="hackathon-dq",
            placeholder="my-data-bucket",
            help="Name of your S3 bucket"
        )
    
    # Step 2.3: Browse and Select File
    if bucket and region:
        st.markdown("**3. Select File from Bucket:**")
        
        # Fetch files from S3
        try:
            with st.spinner(f"Loading files from {bucket}..."):
                response = requests.get(
                    f"{BACKEND_URL}/api/s3/list-files",
                    params={"bucket": bucket},
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    files = data.get("files", [])
                    
                    if files:
                        # Create dropdown with file names and sizes
                        file_options = {f['key']: f"{f['key']} ({f['size']/1024:.1f} KB)" for f in files}
                        selected_file_key = st.selectbox(
                            f"Found {len(files)} files - Select one:",
                            options=list(file_options.keys()),
                            format_func=lambda x: file_options[x],
                            help="Select the file you want to validate"
                        )
                        
                        # Auto-detect file format
                        file_format = selected_file_key.split('.')[-1] if '.' in selected_file_key else 'csv'
                        st.success(f"✅ Selected: `{selected_file_key}` (Format: {file_format})")
                        
                        # Auto-generate dataset name
                        dataset_name = selected_file_key.replace('.csv', '').replace('.parquet', '').replace('/', '_')
                        
                        connection_details = {
                            "bucket": bucket,
                            "key": selected_file_key,
                            "file_format": file_format,
                            "region": region
                        }
                    else:
                        st.warning(f"⚠️ No files found in bucket: {bucket}")
                else:
                    st.error(f"❌ Error accessing bucket: {response.status_code}")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.info("💡 Make sure your AWS credentials are configured in .env file")

# Step 3: Schema Definition
if connection_details and dataset_name:
    st.markdown("---")
    st.markdown("## 3️⃣ Define Schema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        dataset_name_input = st.text_input(
            "Dataset Name:",
            value=dataset_name,
            help="Friendly name for this dataset"
        )
    
    with col2:
        primary_key = st.text_input(
            "Primary Key Column:",
            placeholder="id, customer_id, etc.",
            help="Column that uniquely identifies each row"
        )
    
    required_columns = st.text_input(
        "Required Columns (comma-separated):",
        placeholder="email, name, created_at",
        help="Columns that should not have null values"
    )
    
    # Step 4: Quality Checks
    st.markdown("---")
    st.markdown("## 4️⃣ Select Quality Checks")
    
    st.write("Choose checks to run:")
    
    col1, col2 = st.columns(2)
    with col1:
        check_nulls = st.checkbox("❌ Null Value Check", value=True)
        check_duplicates = st.checkbox("🔄 Duplicate Check", value=True)
    with col2:
        check_freshness = st.checkbox("🕐 Freshness Check", value=True)
        check_volume = st.checkbox("📊 Volume Check", value=True)
    
    quality_checks = []
    if check_nulls:
        quality_checks.append("null_check")
    if check_duplicates:
        quality_checks.append("duplicate_check")
    if check_freshness:
        quality_checks.append("freshness_check")
    if check_volume:
        quality_checks.append("volume_check")
    
    # Step 5: Run Validation
    st.markdown("---")
    st.markdown("## 5️⃣ Run Validation")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🚀 Run Validation", type="primary"):
            # Create configuration
            config_data = {
                "name": dataset_name_input,
                "source_type": source_type,
                "connection_details": connection_details,
                "primary_key": primary_key,
                "required_columns": [c.strip() for c in required_columns.split(',') if c.strip()],
                "quality_checks": quality_checks,
                "is_active": True
            }
            
            # Trigger validation via API
            with st.spinner("🔄 Running validation..."):
                try:
                    response = requests.post(
                        f"{BACKEND_URL}/api/validate",
                        json=config_data,
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        v = result['results']  # validation_results
                        
                        st.success("✅ Validation completed successfully!")
                        st.balloons()
                        
                        # === DETAILED RESULTS LIKE CLI ===
                        st.markdown("="*80)
                        st.markdown("## 🔍 VALIDATION COMPLETE")
                        st.markdown("="*80)
                        
                        # Summary
                        st.markdown("### Summary")
                        s = v['summary']
                       
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Total Checks", s['total_checks'])
                        col2.metric("✅ Passed", s['passed'])
                        col3.metric("❌ Failed", s['failed'])
                        col4.metric("⚠️ Warnings", s.get('warnings', 0))
                        
                        # Quality Score with progress bar
                        score = s['quality_score']
                        emoji = "🟢" if score >= 75 else "🟡" if score >= 50 else "🔴"
                        st.markdown(f"### {emoji} Quality Score: {score}%")
                        st.progress(score / 100)
                        
                        st.markdown("---")
                        st.markdown("### Detailed Results")
                        st.markdown("--------------------------------------------------------------------------------")
                        
                        r = v['results']
                        
                        # NULL CHECK
                        status_icon = "❌" if r['null_check']['status'] == 'FAIL' else "✅"
                        with st.expander(f"{status_icon} NULL CHECK", expanded=True):
                            st.write(f"**Status:** {r['null_check']['status']}")
                            st.write(f"**Total Nulls:** {r['null_check']['total_nulls']}")
                            if r['null_check']['failed_columns']:
                                st.write(f"**Columns with Nulls:** {', '.join(r['null_check']['failed_columns'])}")
                                st.error("⚠️ Data contains null values")
                            else:
                                st.success("✅ No null values - data is clean!")
                        
                        # DUPLICATE CHECK
                        status_icon = "❌" if r['duplicate_check']['status'] == 'FAIL' else "✅"
                        with st.expander(f"{status_icon} DUPLICATE CHECK"):
                            st.write(f"**Status:** {r['duplicate_check']['status']}")
                            st.write(f"**Duplicate Count:** {r['duplicate_check']['duplicate_count']}")
                            st.write(f"**Duplicate %:** {r['duplicate_check']['duplicate_percentage']}%")
                            if r['duplicate_check']['duplicate_count'] == 0:
                                st.success("✅ No duplicates found")
                            else:
                                st.warning(f"⚠️ {r['duplicate_check']['duplicate_count']} duplicates detected")
                        
                        # FRESHNESS CHECK
                        with st.expander(f"{'✅' if r['freshness_check']['status'] == 'PASS' else '⏭️'} FRESHNESS CHECK"):
                            st.write(f"**Status:** {r['freshness_check']['status']}")
                            st.write(f"**Latest Timestamp:** {r['freshness_check']['latest_timestamp']}")
                            st.write(f"**Age (hours):** {r['freshness_check']['age_hours']:.2f}")
                        
                        # VOLUME CHECK
                        with st.expander(f"VOLUME CHECK - {r['volume_check']['status']}"):
                            st.write(f"**Status:** {r['volume_check']['status']}")
                            st.write(f"**Current Count:** {r['volume_check']['current_count']:,} rows")
                            if r['volume_check'].get('message'):
                                st.info(r['volume_check']['message'])
                        
                        st.markdown("="*80)
                        st.markdown(f"### ✅ Data Quality Check Complete!")
                        st.markdown("="*80)
                        
                        # Dataset info
                        st.info(f"""
                        **Dataset:** {v['dataset']}  
                        **Source:** {v['source']}  
                        **Total Rows:** {v['row_count']:,}  
                        **Timestamp:** {v['timestamp']}  
                        **Saved to:** `{result['source_id']}`
                        """)
                        
                        # Next steps
                        st.markdown("### 🎯 Next Steps:")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("• Results saved to S3 for AI chatbot reference")
                            st.markdown("• Go to **Dashboard** to view detailed results")
                        with col2:
                            st.markdown("• Go to **Chatbot** to ask questions")
                            if score < 75:
                                st.warning(f"⚠️ Quality score ({score}%) below 75%")
                    else:
                        st.error(f"❌ Validation failed: {response.text}")
                except requests.exceptions.Timeout:
                    st.error("❌ Validation timed out")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

else:
    st.info("👆 Complete the configuration above to run validation")

# Help Section
with st.expander("❓ Need Help?"):
    st.markdown("""
    **How to use:**
    1. Select your data source (currently S3 only)
    2. Configure connection details (bucket, region, file)
    3. Define schema (primary key, required columns)
    4. Select quality checks to run
    5. Click "Run Validation" to execute checks
    
    **Results will show:**
    - Quality score (0-100%)
    - Detailed check results
    - Issues found and recommendations
    """)
