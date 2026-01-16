# Agent Architecture - How Decisions Are Made

## Overview
Our data quality system uses a **hybrid approach** combining:
1. **AI/LLM (OpenAI GPT-3.5-turbo)** - For intelligent, context-aware decisions
2. **Data-Driven Analysis** - Statistical pattern detection from actual data
3. **Deterministic Rules** - Fast, reliable checks for simple cases
4. **Agent Framework** - Specialized agents for different issue types

---

## Decision-Making Stack

### 1. **AI/LLM Layer (OpenAI GPT-3.5-turbo)**
**What it does:**
- Makes intelligent, context-aware decisions
- Understands data patterns and suggests fixes
- Validates company names, emails, dates
- Infers missing information (e.g., country from city)

**Where it's used:**
- Email validation and fixing
- Company name validation
- Geographic enrichment (city → country)
- Date normalization (ambiguous formats)
- Phone number normalization (complex formats)
- Date column relationship detection (birth date vs job start)

**How it works:**
```python
# Example: Email validation agent uses LLM
response = llm.client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "You are an intelligent email validation assistant..."},
        {"role": "user", "content": f"Fix this email: {email}. Data context: {data_context}"}
    ],
    temperature=0.2
)
```

**AI makes decisions on:**
- ✅ What's the correct email format? (adds @gmail.com, fixes typos)
- ✅ Is this company name valid? (validates against real companies)
- ✅ What country is this city in? (geographic knowledge)
- ✅ Which date column is birth date? (understands column relationships)
- ✅ How to normalize this phone number? (country-specific formatting)

---

### 2. **Data-Driven Analysis Layer (DataAnalyzer)**
**What it does:**
- Analyzes actual data patterns (not hardcoded rules)
- Learns from the dataset (most common values, domains, countries)
- Detects column types from data (email, phone, date, etc.)
- Finds relationships between columns

**Where it's used:**
- Column type detection (email, phone, date, company)
- Most common value detection (company names, email domains)
- Country detection from phone patterns
- Data context for LLM prompts

**How it works:**
```python
# Example: Detects email columns from data patterns
column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)
# Finds columns where >50% values contain '@' and domain pattern
# Learns most common email domain from actual data
```

**Data analysis provides:**
- ✅ Which columns are emails? (by data pattern, not just name)
- ✅ What's the most common email domain? (learns from data)
- ✅ What country are these phones from? (analyzes patterns)
- ✅ Which columns are dates? (detects date patterns)

---

### 3. **Deterministic Rules Layer**
**What it does:**
- Fast, reliable checks for simple cases
- Pattern matching (regex, format validation)
- Unit conversions (mathematical)
- Basic data cleaning

**Where it's used:**
- Email format validation (regex)
- Phone number format detection (regex)
- Unit conversions (cm to m, kg to lb)
- Date parsing (common formats)

**How it works:**
```python
# Example: Phone normalization (deterministic)
if country_code == 'IN' and len(digits) == 10:
    normalized = f"+91 {digits}"  # Indian format
```

**Deterministic rules handle:**
- ✅ Simple format validation (regex patterns)
- ✅ Mathematical conversions (units, dates)
- ✅ Fast checks before calling expensive LLM

---

### 4. **Agent Framework**
**What it does:**
- Specialized agents for different issue types
- Orchestrates all agents to run together
- Combines AI + Data Analysis + Rules

**Agents:**
1. **EmailValidationAgent** - Uses AI + Data Analysis
2. **FormattingAgent** - Uses AI + Deterministic Rules
3. **CompanyValidationAgent** - Uses AI + Data Analysis
4. **GeographicEnrichmentAgent** - Uses AI
5. **UnitsAgent** - Uses AI + Deterministic Rules
6. **CategoricalAgent** - Uses AI
7. **ImputationAgent** - Uses AI
8. **SemanticAgent** - Uses AI
9. **LogicAgent** - Uses AI + Data Analysis
10. **ExtractionAgent** - Uses AI

---

## Decision Flow Example: Email Validation

```
1. DataAnalyzer analyzes dataset
   └─> Finds columns with >50% email patterns
   └─> Learns most common email domain (e.g., "@company.com")

2. EmailValidationAgent processes each email
   └─> Deterministic: Regex check for basic format
   └─> If invalid:
       └─> AI/LLM: "Fix this email: 'john.doe'. 
                    Most common domain in dataset: '@company.com'"
       └─> AI decides: "john.doe@company.com"
       └─> Creates issue with AI-suggested fix

3. User applies fix
   └─> Email is corrected to AI-suggested value
```

---

## Decision Flow Example: Company Validation

```
1. DataAnalyzer analyzes dataset
   └─> Finds company columns (high uniqueness, text type)
   └─> Counts all company names
   └─> Finds most common company name

2. CompanyValidationAgent processes each company
   └─> For each unique company name:
       └─> AI/LLM: "Is 'Microsft' valid? Most common in dataset: 'Microsoft'"
       └─> AI decides: "Invalid, correct to 'Microsoft'"
       └─> Creates issue for all occurrences

3. User applies fix
   └─> All variations standardized to most common/correct name
```

---

## Decision Flow Example: Phone Normalization

```
1. DataAnalyzer analyzes dataset
   └─> Detects phone columns from data patterns
   └─> Analyzes phone numbers to detect country
   └─> Finds: 80% have +91 prefix → Country = India

2. FormattingAgent processes each phone
   └─> Deterministic: Format as +91 XXXXXXXXXX (Indian format)
   └─> If complex format:
       └─> AI/LLM: "Normalize this phone: '91-98765-43210'. Country: India"
       └─> AI decides: "+91 9876543210"
       └─> Creates issue with normalized value

3. User applies fix
   └─> Phone normalized to country-specific format
```

---

## What Makes It "AI-Powered"?

### ✅ **AI Makes Intelligent Decisions:**
- Understands context (e.g., "this looks like a typo")
- Learns from data (e.g., "most emails use @company.com")
- Validates against knowledge (e.g., "Microsoft is a real company")
- Suggests fixes (e.g., "add @gmail.com here")

### ✅ **Data-Driven (Not Hardcoded):**
- Adapts to any dataset structure
- Learns patterns from actual data
- Works regardless of column names
- Makes decisions based on what it finds

### ✅ **Combines Multiple Approaches:**
- Fast deterministic rules for simple cases
- AI for complex, context-aware decisions
- Data analysis to learn patterns
- Agents specialized for different issue types

---

## Current AI Model

**Model:** OpenAI GPT-3.5-turbo  
**API:** OpenAI Chat Completions API  
**Usage:** ~10-15 LLM calls per validation run (one per agent type)  
**Cost:** ~$0.001-0.002 per validation (very cheap)

---

## Summary

**We use:**
1. **AI (OpenAI GPT-3.5-turbo)** - For intelligent decisions ✅
2. **Agents** - Specialized Python classes that use AI ✅
3. **Data Analysis** - Learns from your actual data ✅
4. **Deterministic Rules** - Fast checks for simple cases ✅

**Decisions are made by:**
- **AI** when context/intelligence is needed (emails, companies, dates, geography)
- **Data Analysis** to learn patterns from your dataset
- **Deterministic Rules** for fast, reliable format checks

**Result:** Intelligent, adaptive, data-driven data quality system! 🚀
