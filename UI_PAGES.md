# UI Pages Configuration

## 📊 Visible Pages (Active in Sidebar)

### 1. **Chatbot** (`1_Chatbot.py`)
   - AI-powered chatbot for querying data quality information
   - Ask questions about datasets, validation results, and metrics
   - Uses LLM (Gemini/OpenAI/Claude) to answer queries

### 2. **Agentic Issues** (`2_Agentic_Issues.py`)
   - **PRIMARY FEATURE** - Main data quality platform
   - Upload CSV files from S3
   - Run AI-powered validation
   - View detected issues in table format
   - Select issues to fix
   - Preview cleaned CSV with highlighted changes
   - Download or save cleaned data to S3

---

## 🔒 Hidden Pages (Not Visible, Not Deleted)

Location: `frontend/.hidden_pages/`

### 1. **Configure** (`1_Configure.py`)
   - Dataset configuration page
   - **Status:** Hidden but preserved
   - **Location:** `frontend/.hidden_pages/1_Configure.py`

### 2. **Dashboard** (`2_Dashboard.py`)
   - Dashboard with metrics visualization
   - **Status:** Hidden but preserved
   - **Location:** `frontend/.hidden_pages/2_Dashboard.py`

---

## 📂 Frontend Structure

```
frontend/
├── app.py                    # Main Streamlit app (Home)
├── pages/                    # VISIBLE pages
│   ├── 1_Chatbot.py         # ✅ Visible
│   └── 2_Agentic_Issues.py  # ✅ Visible (PRIMARY FEATURE)
├── .hidden_pages/            # HIDDEN pages (preserved)
│   ├── 1_Configure.py       # 🔒 Hidden
│   └── 2_Dashboard.py       # 🔒 Hidden
└── requirements.txt
```

---

## 🔄 To Restore Hidden Pages:

If you want to make Configure or Dashboard visible again:

```bash
cd /Users/kunal.khedkar/Desktop/Hackethon_bot/Hackathon_DQ/frontend

# Restore Configure
mv .hidden_pages/1_Configure.py pages/1_Configure.py

# Restore Dashboard  
mv .hidden_pages/2_Dashboard.py pages/2_Dashboard.py

# Renumber other pages accordingly
mv pages/1_Chatbot.py pages/3_Chatbot.py
mv pages/2_Agentic_Issues.py pages/4_Agentic_Issues.py

# Restart frontend
streamlit run app.py
```

---

## ✅ Current UI Navigation

**Sidebar Menu:**
```
📱 Home (app.py)
  └─ 1️⃣ Chatbot
  └─ 2️⃣ Agentic Issues ⭐ (Main Feature)
```

**Hidden (but preserved):**
- 🔒 Configure
- 🔒 Dashboard

---

## 🎯 Benefits of Current Setup:

1. ✅ **Simplified UI** - Only essential pages visible
2. ✅ **Preserved Code** - Hidden pages still exist, not deleted
3. ✅ **Easy to Restore** - Just move files back to `pages/`
4. ✅ **Clean Navigation** - Focus on main feature (Agentic Issues)
5. ✅ **No Breaking Changes** - All code intact, just organization changed
