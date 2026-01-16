# Column Detection: Auto-Detection vs Predefined

## Current Implementation: **HYBRID APPROACH** (Auto-Detection + Fallback)

The system uses **BOTH** methods, with **auto-detection from data patterns as PRIMARY**:

### 1. **Email Columns** 
**Primary (Auto-Detection):**
- Analyzes actual data values
- If >50% of values contain `@` and domain pattern → Detected as email
- **Works regardless of column name**

**Fallback (Name-based):**
- Also checks if column name contains: 'email', 'e-mail', 'mail'
- Only used if data pattern detection doesn't work

**Example:**
- Column named "contact_info" with values like "john@company.com" → **Auto-detected as email** ✅
- Column named "email" → Detected by name ✅

---

### 2. **Phone Columns**
**Primary (Auto-Detection):**
- Analyzes actual data values
- If >30% match phone patterns (`+91`, `+1`, 10+ digits) → Detected as phone
- **Works regardless of column name**

**Fallback (Name-based):**
- Also checks if column name contains: 'phone', 'tel', 'mobile', 'cell'
- Only used if data pattern detection doesn't work

**Example:**
- Column named "contact" with values like "+91 9876543210" → **Auto-detected as phone** ✅
- Column named "phone_number" → Detected by name ✅

---

### 3. **Date Columns**
**Primary (Auto-Detection):**
- Analyzes actual data values
- If >30% match date patterns (`YYYY-MM-DD`, `MM/DD/YYYY`, etc.) → Detected as date
- **Works regardless of column name**

**Fallback (Name-based):**
- Also checks if column name contains: 'date', 'time', 'created', 'updated', 'timestamp', 'dob', 'birth', 'start', 'end'
- Only used if data pattern detection doesn't work

**Example:**
- Column named "event" with values like "2024-01-15" → **Auto-detected as date** ✅
- Column named "birth_date" → Detected by name ✅

---

### 4. **Company Columns**
**Primary (Auto-Detection):**
- Analyzes data characteristics:
  - High uniqueness (10+ unique values)
  - Text type (not numeric)
  - Not email/phone type
  - Moderate uniqueness (not too common, not too unique)
- **Works regardless of column name**

**Fallback (Name-based):**
- Also checks if column name contains: 'company', 'organisation', 'organization', 'org', 'corp', 'firm'
- Only used if data pattern detection doesn't work

**Example:**
- Column named "employer" with values like "TCS", "Infosys", "Wipro" → **Auto-detected as company** ✅
- Column named "company_name" → Detected by name ✅

---

## How It Works

### Step 1: Data Analysis (Auto-Detection)
```python
# DataAnalyzer analyzes ALL columns
column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)

# For each column, it:
# 1. Samples values (up to 1000 rows)
# 2. Checks data patterns:
#    - Email: >50% contain @ and domain pattern
#    - Phone: >30% match phone patterns
#    - Date: >30% match date patterns
#    - Company: High uniqueness, text type
# 3. Assigns type: 'email', 'phone', 'date', 'company', etc.
```

### Step 2: Agent Processing
```python
# Each agent checks:
# 1. PRIMARY: Is this column's detected type matching? (auto-detection)
# 2. FALLBACK: Does column name match keywords? (name-based)

if (analysis.get('type') == 'email' or  # AUTO-DETECTED
    any(kw in col_lower for kw in ['email', 'e-mail', 'mail'])):  # FALLBACK
    # Process as email column
```

---

## Summary

| Column Type | Primary Method | Fallback Method |
|------------|---------------|-----------------|
| **Email** | Auto-detect: >50% values have @ and domain | Name contains: 'email', 'mail' |
| **Phone** | Auto-detect: >30% values match phone patterns | Name contains: 'phone', 'tel', 'mobile' |
| **Date** | Auto-detect: >30% values match date patterns | Name contains: 'date', 'time', 'dob', etc. |
| **Company** | Auto-detect: High uniqueness, text type | Name contains: 'company', 'org', 'corp' |

---

## Answer to Your Question

**Are column fields predefined or auto-detected?**

**Answer: BOTH, but AUTO-DETECTION is PRIMARY**

- ✅ **Primary**: Auto-detection from actual data patterns (works with ANY column name)
- ✅ **Fallback**: Name-based detection (only if data patterns aren't clear)

**This means:**
- ✅ Works with any column names (e.g., "contact_info" → auto-detected as email)
- ✅ Adapts to your data structure
- ✅ Not hardcoded to specific column names
- ✅ Falls back to name-based if data is ambiguous

**The system is truly data-driven!** 🚀
