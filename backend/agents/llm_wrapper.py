"""
LLM Wrapper - Provides unified interface for LLM calls (Gemini) with automatic model fallback
"""
import json
from typing import Optional, Dict, Any, List
import google.genai as genai
import signal
from contextlib import contextmanager


# List of all Gemini models to try in order (fastest/cheapest first)
GEMINI_MODELS_FALLBACK = [
    'gemini-flash-lite-latest',
    'gemini-2.5-flash-lite',
    'gemini-2.0-flash-lite',
    'gemini-2.0-flash-lite-001',
    'gemini-2.0-flash-lite-preview',
    'gemini-flash-latest',
    'gemini-2.5-flash',
    'gemini-2.0-flash',
    'gemini-2.0-flash-001',
    'gemini-2.0-flash-exp',
    'gemini-2.5-pro',
    'gemini-3-flash-preview',
    'gemini-3-pro-preview',
    'gemini-pro-latest',
]


class LLMWrapper:
    """Wrapper around Google Gemini to provide OpenAI-like interface for agents with automatic model fallback"""
    
    def __init__(self, api_key: str, model: str = "gemini-flash-lite-latest"):
        """
        Initialize LLM wrapper
        
        Args:
            api_key: Google Gemini API key
            model: Primary model to use (default: gemini-flash-lite-latest)
        """
        self.api_key = api_key
        self.primary_model = model
        self.current_model = model
        self.client = genai.Client(api_key=api_key)
        self.failed_models = set()  # Track models that failed (only permanent failures like 404)
        self.quota_exhausted_models = set()  # Track models with quota exhausted (reset on new session)
    
    def _get_models_to_try(self) -> List[str]:
        """Get list of models to try, starting with current model, then fallback list"""
        models_to_try = [self.current_model] if (self.current_model not in self.failed_models and 
                                                   self.current_model not in self.quota_exhausted_models) else []
        # Add other models from fallback list, excluding failed and quota-exhausted ones
        for model in GEMINI_MODELS_FALLBACK:
            if (model not in self.failed_models and 
                model not in self.quota_exhausted_models and 
                model not in models_to_try):
                models_to_try.append(model)
        return models_to_try
    
    def chat_completions_create(
        self,
        messages: list,
        temperature: float = 0.2,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        Create chat completion (OpenAI-compatible interface) with automatic model fallback
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            
        Returns:
            Dict with 'choices' containing response
        """
        # Convert messages to Gemini format
        # Combine system and user messages
        text_parts = []
        for msg in messages:
            role = msg.get('role', 'user')
            content = msg.get('content', '')
            if role == 'system':
                text_parts.append(f"System: {content}")
            elif role == 'user':
                text_parts.append(f"User: {content}")
            elif role == 'assistant':
                text_parts.append(f"Assistant: {content}")
        
        prompt = "\n\n".join(text_parts)
        
        # Try models in order until one works
        models_to_try = self._get_models_to_try()
        
        # If too many models are exhausted, limit attempts to speed up
        if len(self.quota_exhausted_models) >= 10:
            # Only try the first 3 models to avoid long delays
            models_to_try = models_to_try[:3]
            print(f"⚠️ Many models exhausted ({len(self.quota_exhausted_models)}), limiting to first 3 models for speed")
        
        for model in models_to_try:
            try:
                print(f"🔄 Trying Gemini model: {model}")
                # Add timeout to prevent long waits (5 seconds per model)
                import time
                start_time = time.time()
                response = self.client.models.generate_content(
                    model=f'models/{model}',
                    contents=prompt,
                    config={
                        'temperature': temperature,
                        'max_output_tokens': max_tokens
                    }
                )
                elapsed = time.time() - start_time
                if elapsed > 2:
                    print(f"⚠️ Model {model} took {elapsed:.1f}s (slow)")
                
                # Extract text from response - try multiple methods
                response_text = None
                
                # Method 1: Direct .text property (most common for Gemini)
                try:
                    if hasattr(response, 'text'):
                        # response.text might be a property, try calling it
                        text_value = response.text
                        if text_value:
                            response_text = str(text_value).strip()
                            if response_text:
                                print(f"DEBUG: Extracted from response.text: {response_text[:100]}")
                except Exception as e:
                    print(f"DEBUG: Error accessing response.text: {e}")
                
                # Method 2: From candidates[0].content.parts[0].text
                if not response_text or response_text == '':
                    try:
                        if hasattr(response, 'candidates') and response.candidates and len(response.candidates) > 0:
                            candidate = response.candidates[0]
                            if hasattr(candidate, 'content'):
                                content = candidate.content
                                if hasattr(content, 'parts') and content.parts and len(content.parts) > 0:
                                    part = content.parts[0]
                                    # Try both .text attribute and direct access
                                    if hasattr(part, 'text'):
                                        text_value = part.text
                                        if text_value:
                                            response_text = str(text_value).strip()
                                            if response_text:
                                                print(f"DEBUG: Extracted from candidates.parts[0].text: {response_text[:100]}")
                    except Exception as e:
                        print(f"DEBUG: Error accessing candidates: {e}")
                
                # Ensure we have valid text
                if not response_text or response_text.strip() == '':
                    print(f"DEBUG: Empty response from model {model}")
                    print(f"DEBUG: Response type: {type(response)}")
                    print(f"DEBUG: Has text: {hasattr(response, 'text')}")
                    if hasattr(response, 'candidates'):
                        print(f"DEBUG: Has candidates: {len(response.candidates) if response.candidates else 0}")
                    raise ValueError(f"Empty response from model {model}")
                
                # Success! Update current model and return
                if self.current_model != model:
                    print(f"✅ Switched to working model: {model}")
                    self.current_model = model
                
                # Return OpenAI-compatible format
                return {
                    'choices': [{
                        'message': {
                            'content': response_text
                        }
                    }],
                    'model': model
                }
                
            except Exception as e:
                error_str = str(e)
                error_type = type(e).__name__
                # Check if it's a quota error or permanent error
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower():
                    print(f"⚠️ Model {model} quota exhausted, skipping...")
                    # Mark as quota exhausted for this session (don't retry immediately)
                    self.quota_exhausted_models.add(model)
                    continue  # Try next model quickly
                elif '404' in error_str or 'not found' in error_str.lower():
                    print(f"⚠️ Model {model} not found, skipping...")
                    self.failed_models.add(model)  # Only mark 404 as permanently failed
                    continue
                elif 'ValueError' in error_type and 'Empty response' in error_str:
                    # Empty response - might be a transient issue, try next model
                    print(f"⚠️ Model {model} returned empty response, trying next model...")
                    continue
                else:
                    print(f"⚠️ Model {model} error ({error_type}): {error_str[:100]}, trying next model...")
                    # Don't mark as failed for transient errors
                    continue
        
        # All models failed
        print(f"❌ All Gemini models failed. Tried: {models_to_try}")
        print(f"⚠️ WARNING: LLM is not available. Geographic enrichment will have limited functionality.")
        print(f"   Failed models: {list(self.failed_models)}")
        print(f"   Quota exhausted models: {list(self.quota_exhausted_models)}")
        return {
            'choices': [{
                'message': {
                    'content': None
                }
            }],
            'model': self.current_model
        }


# Compatibility: Make QueryEngine work as LLM client for agents
def create_llm_client_from_query_engine(query_engine) -> LLMWrapper:
    """Create LLM wrapper from QueryEngine instance"""
    # QueryEngine now uses current_model instead of model
    model = getattr(query_engine, 'current_model', None) or getattr(query_engine, 'primary_model', None) or 'gemini-flash-lite-latest'
    return LLMWrapper(
        api_key=query_engine.api_key,
        model=model
    )
