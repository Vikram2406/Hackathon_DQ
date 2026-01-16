"""
Helper function for agents to call LLM (works with both OpenAI and Gemini)
"""
import json
from typing import Optional, Dict, Any, List


def call_llm(
    llm,
    messages: List[Dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 1000
) -> Optional[str]:
    """
    Unified LLM call function that works with both OpenAI and Gemini
    
    Args:
        llm: LLM client (QueryEngine, LLMWrapper, or OpenAI client)
        messages: List of message dicts with 'role' and 'content'
        temperature: Temperature for generation
        max_tokens: Maximum tokens to generate
        
    Returns:
        Response text or None if error
    """
    try:
        # Check if it's our LLMWrapper (Gemini) or ClaudeWrapper
        if hasattr(llm, 'chat_completions_create'):
            response = llm.chat_completions_create(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            content = response['choices'][0]['message']['content']
            return content if content else None
        
        # Check if it's OpenAI-style client
        elif hasattr(llm, 'client') and hasattr(llm.client, 'chat'):
            response = llm.client.chat.completions.create(
                model=llm.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
        
        # Direct OpenAI client
        elif hasattr(llm, 'chat') and hasattr(llm.chat, 'completions'):
            response = llm.chat.completions.create(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
        
        else:
            print(f"⚠️ Unknown LLM client type: {type(llm)}")
            return None
            
    except Exception as e:
        error_str = str(e)
        # Check for quota errors (works for both OpenAI and Gemini)
        if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower() or 'rate_limit' in error_str.lower():
            print(f"❌ LLM Quota/Rate Limit Exhausted: {error_str[:200]}")
            print("⚠️ LLM API quota/rate limit reached. Please check your API account limits.")
        else:
            print(f"❌ Error calling LLM: {e}")
            import traceback
            traceback.print_exc()
        return None
