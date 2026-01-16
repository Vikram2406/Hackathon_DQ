"""
AI Chatbot Page - Fully AI-Powered with File Support
"""
import streamlit as st
import requests
import os
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

st.set_page_config(page_title="AI Chatbot", page_icon="💬", layout="wide")

st.title("💬 AI Data Quality Assistant")
st.markdown("Ask questions about your data quality in natural language. You can ask about specific files!")

# Sidebar with available files
with st.sidebar:
    st.markdown("### 📁 Available Files")
    try:
        response = requests.get(f"{BACKEND_URL}/api/chat/files", timeout=5)
        if response.status_code == 200:
            files_data = response.json()
            available_files = files_data.get("files", [])
            
            if available_files:
                st.success(f"✅ {len(available_files)} file(s) available")
                for file in available_files[:10]:  # Show first 10
                    st.text(f"• {file}")
                if len(available_files) > 10:
                    st.caption(f"... and {len(available_files) - 10} more")
            else:
                st.warning("No files found")
                st.info("Run validation first to generate results")
        else:
            st.error("Could not load files")
    except Exception as e:
        st.error(f"Error: {str(e)}")
    
    st.markdown("---")
    st.markdown("### 💡 Example Questions")
    st.markdown("""
    - "What is the data quality in customers.csv?"
    - "Tell me about null values in orders.json"
    - "What issues are in my_data.parquet?"
    - "Is the data quality good in customers.csv?"
    - "What should I fix in orders.json?"
    """)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about data quality (e.g., 'What is the data quality in customers.csv?')"):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get AI response from backend
    with st.chat_message("assistant"):
        with st.spinner("🤔 Thinking..."):
            try:
                response = requests.post(
                    f"{BACKEND_URL}/api/chat",
                    json={
                        "query": prompt,
                        "file_name": None  # Backend will extract from query
                    },
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    ai_response = data.get("response", "I'm sorry, I couldn't generate a response.")
                    
                    # Display response
                    st.markdown(ai_response)
                    
                    # Add to chat history
                    st.session_state.messages.append({"role": "assistant", "content": ai_response})
                    
                elif response.status_code == 503:
                    error_msg = response.json().get("detail", "Chatbot service unavailable")
                    st.error(f"❌ {error_msg}")
                    st.info("Please configure OPENAI_API_KEY in your .env file to enable AI features.")
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": f"❌ {error_msg}\n\nPlease configure OPENAI_API_KEY to enable AI features."
                    })
                else:
                    error_msg = response.json().get("detail", "Unknown error")
                    st.error(f"❌ Error: {error_msg}")
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": f"❌ Error: {error_msg}"
                    })
                    
            except requests.exceptions.Timeout:
                st.error("⏱️ Request timed out. Please try again.")
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "⏱️ Request timed out. Please try again."
                })
            except requests.exceptions.ConnectionError:
                st.error("❌ Could not connect to backend. Make sure the backend is running.")
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "❌ Could not connect to backend. Make sure the backend is running on http://localhost:8000"
                })
            except Exception as e:
                st.error(f"❌ Unexpected error: {str(e)}")
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"❌ Unexpected error: {str(e)}"
                })

# Clear chat button
if st.button("🗑️ Clear Chat"):
    st.session_state.messages = []
    st.rerun()

# Info section
with st.expander("ℹ️ How to use"):
    st.markdown("""
    **Ask questions in natural language:**
    
    1. **File-specific questions:**
       - "What is the data quality in customers.csv?"
       - "Tell me about null values in orders.json"
       - "What issues are in my_data.parquet?"
    
    2. **General questions:**
       - "Is the data quality good?"
       - "What should I fix first?"
       - "Are there any duplicates?"
    
    3. **The AI will:**
       - Automatically find the file you mention
       - Load validation results from that file
       - Answer based on the actual data quality metrics
       - Provide actionable recommendations
    
    **Note:** Make sure to run validation first to generate data quality reports!
    """)
