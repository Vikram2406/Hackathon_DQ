"""
AI Chatbot Page - Read Results from S3
"""
import streamlit as st
import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

st.set_page_config(page_title="AI Chatbot", page_icon="💬", layout="wide")

st.title("💬 AI Data Quality Assistant")
st.markdown("Ask questions about your data quality in natural language")

# Function to read results from S3
def get_latest_results_from_s3():
    """Read latest validation results from S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/')
        
        # List all latest.json files
        response = s3_client.list_objects_v2(
            Bucket=results_bucket,
            Prefix=results_prefix
        )
        
        datasets = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('latest.json'):
                    # Read the JSON file
                    result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                    data = json.loads(result['Body'].read().decode('utf-8'))
                    datasets.append(data)
        
        return datasets
    except Exception as e:
        st.error(f"Error reading from S3: {str(e)}")
        return []

# Get datasets from S3
datasets = get_latest_results_from_s3()

if not datasets:
    st.warning("🔍 No datasets found! Run validation first.")
    st.info("""
    Run: `cd backend && source venv/bin/activate && cd .. && python3 run_validation_simple.py`
    
    Then come back here to chat!
    """)
    st.stop()

# Dataset selector
dataset_names = [d['dataset'] for d in datasets]
selected_dataset = st.selectbox("Select Dataset:", dataset_names)

# Get dataset data
dataset_data = next((d for d in datasets if d['dataset'] == selected_dataset), None)

if dataset_data:
    st.success(f"✅ Loaded results for: **{selected_dataset}**")
    
    # Quick stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Quality Score", f"{dataset_data['summary']['quality_score']}%")
    with col2:
        st.metric("Checks Passed", dataset_data['summary']['passed'])
    with col3:
        st.metric("Checks Failed", dataset_data['summary']['failed'])
    
    st.markdown("---")
    
    # Suggested questions
    st.markdown("### 💡 Try asking:")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("What issues were found?"):
            st.session_state.prompt = "What data quality issues were found?"
        if st.button("Which columns have problems?"):
            st.session_state.prompt = "Which columns have null values?"
    
    with col2:
        if st.button("Is the data good quality?"):
            st.session_state.prompt = "Is the overall data quality acceptable?"
        if st.button("What should I fix first?"):
            st.session_state.prompt = "What are the most critical issues to fix?"
    
    # Chat interface
    st.markdown("---")
    st.markdown("### 💬 Chat")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about data quality..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate response based on S3 data
        with st.chat_message("assistant"):
            # Simple rule-based responses using S3 data
            results = dataset_data['results']
            
            if "issue" in prompt.lower() or "problem" in prompt.lower() or "found" in prompt.lower():
                response = f"""Based on the latest validation of **{selected_dataset}**:
                
**Issues Found:**
- **Null Values:** {results['null_check']['total_nulls']} nulls in columns: {', '.join(results['null_check']['failed_columns']) if results['null_check']['failed_columns'] else 'None'}
- **Duplicates:** {results['duplicate_check']['duplicate_count']} duplicate records
- **Overall Quality Score:** {dataset_data['summary']['quality_score']}%

**Recommendation:** {'Address the null values first, especially in ' + ', '.join(results['null_check']['failed_columns']) if results['null_check']['failed_columns'] else 'Data quality looks good!'}
                """
            
            elif "null" in prompt.lower() or "column" in prompt.lower():
                if results['null_check']['failed_columns']:
                    response = f"""Columns with null values:
                    
{chr(10).join([f"- **{col}**" for col in results['null_check']['failed_columns']])}

Total null values: **{results['null_check']['total_nulls']}**

These should be investigated to determine if they're expected or data quality issues.
                    """
                else:
                    response = "✅ No null values found! All columns have complete data."
            
            elif "quality" in prompt.lower() or "acceptable" in prompt.lower():
                score = dataset_data['summary']['quality_score']
                response = f"""**Overall Data Quality Assessment:**

Quality Score: **{score}%**

{'✅ Excellent! Data quality is very good.' if score >= 75 else '⚠️ Needs attention. Quality below acceptable threshold.' if score >= 50 else '❌ Critical. Significant data quality issues detected.'}

**Details:**
- Checks Passed: {dataset_data['summary']['passed']}/{dataset_data['summary']['total_checks']}
- Failed Checks: {dataset_data['summary']['failed']}
- Total Rows: {dataset_data['row_count']:,}
                """
            
            elif "fix" in prompt.lower() or "critical" in prompt.lower():
                response = f"""**Priority Issues to Fix:**

1. **Null Values** ({results['null_check']['total_nulls']} found)
   - Columns affected: {', '.join(results['null_check']['failed_columns']) if results['null_check']['failed_columns'] else 'None'}
   - Action: Investigate source data quality

{'2. **Duplicates** (' + str(results['duplicate_check']['duplicate_count']) + ' found)' if results['duplicate_check']['duplicate_count'] > 0 else ''}
   - Action: Review primary key definition

Start with nulls as they have the most impact on data usability.
                """
            
            else:
                response = f"""I can help you understand the data quality for **{selected_dataset}**.

**Current Status:**
- Quality Score: {dataset_data['summary']['quality_score']}%
- Total Rows: {dataset_data['row_count']:,}
- Last Validated: {dataset_data['timestamp']}

Try asking:
- "What issues were found?"
- "Which columns have problems?"
- "What should I fix first?"
                """
            
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Clear chat
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()
