"""
OpenAI Wrapper - Provides unified interface for OpenAI API calls
"""
import json
from typing import Optional, Dict, Any, List
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class OpenAIWrapper:
    """Wrapper around OpenAI to provide unified interface for agents"""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize OpenAI wrapper
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini - fast and cost-effective)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI package not installed. Run: pip install openai")
        
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key)
    
    def chat_completions_create(
        self,
        messages: list,
        temperature: float = 0.2,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        Create chat completion (unified interface)
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            
        Returns:
            Dict with 'choices' containing response
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            # Extract text from response
            response_text = response.choices[0].message.content
            
            # Return unified format
            return {
                'choices': [{
                    'message': {
                        'content': response_text
                    }
                }],
                'model': self.model
            }
        except Exception as e:
            error_str = str(e)
            print(f"❌ Error in OpenAI API call: {error_str[:200]}")
            return {
                'choices': [{
                    'message': {
                        'content': None
                    }
                }],
                'model': self.model
            }
