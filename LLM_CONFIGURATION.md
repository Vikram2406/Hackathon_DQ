# LLM Provider Configuration Guide

## Overview
The system supports **two LLM providers** that can be easily switched:
- **OpenAI** (default) - Uses GPT models (gpt-4o-mini, gpt-3.5-turbo, etc.)
- **Gemini** - Uses Google Gemini models (gemini-flash-lite-latest, gemini-2.5-flash, etc.)

## How to Switch Providers

### Method 1: Environment Variable (Recommended)
```bash
# Use OpenAI (default)
export LLM_PROVIDER=openai
export OPENAI_API_KEY=your_openai_key_here

# Use Gemini
export LLM_PROVIDER=gemini
export GOOGLE_API_KEY=your_gemini_key_here
```

### Method 2: In Code
```python
from agents.llm_provider import LLMProviderFactory, LLMProvider

# Force OpenAI
llm_client = LLMProviderFactory.create_llm_client(LLMProvider.OPENAI)

# Force Gemini
llm_client = LLMProviderFactory.create_llm_client(LLMProvider.GEMINI)
```

## Current Configuration

**Default Provider:** OpenAI  
**Model:** gpt-4o-mini (fast, cost-effective)

## API Keys

### OpenAI
- **Key Location:** Set `OPENAI_API_KEY` environment variable
- **Current Key:** Configured in code (premium key)
- **Models Available:** gpt-4o-mini, gpt-3.5-turbo, gpt-4, etc.

### Gemini
- **Key Location:** Set `GOOGLE_API_KEY` or `GEMINI_API_KEY` environment variable
- **Models Available:** gemini-flash-lite-latest, gemini-2.5-flash, gemini-3-flash-preview, etc.
- **Note:** Has automatic fallback across 14 models

## Architecture

### LLM Provider Factory
- **File:** `backend/agents/llm_provider.py`
- **Purpose:** Centralized factory for creating LLM clients
- **Methods:**
  - `get_provider()` - Returns current provider (OPENAI or GEMINI)
  - `create_llm_client()` - Creates unified LLM client for agents
  - `create_query_engine()` - Creates QueryEngine for chatbot

### Unified Interface
Both providers implement the same interface:
- `chat_completions_create(messages, temperature, max_tokens)` - Returns OpenAI-compatible format
- Works seamlessly with all agents

### Agents Using LLM
All agents use the unified interface via `llm_helper.call_llm()`:
- EmailValidationAgent
- CompanyValidationAgent
- GeographicEnrichmentAgent
- FormattingAgent
- UnitsAgent
- CategoricalAgent
- ImputationAgent
- SemanticAgent
- LogicAgent
- ExtractionAgent

## Switching Providers

### To Switch to Gemini:
```bash
export LLM_PROVIDER=gemini
export GOOGLE_API_KEY=your_gemini_key
# Restart backend
```

### To Switch to OpenAI:
```bash
export LLM_PROVIDER=openai
export OPENAI_API_KEY=your_openai_key
# Restart backend
```

## Benefits

1. **Easy Switching:** Change one environment variable
2. **Unified Interface:** All agents work with both providers
3. **Fallback Support:** Gemini has automatic model fallback
4. **Cost Optimization:** Choose provider based on cost/performance needs
5. **Quota Management:** Switch providers if one hits quota limits

## Current Status

✅ **OpenAI** is configured and working
- Premium API key set
- All agents tested and working
- Geographic enrichment working correctly
