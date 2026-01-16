"""
Claude Wrapper - Provides unified interface for Anthropic Claude API calls
"""
import json
from typing import Optional, Dict, Any, List
import anthropic


class ClaudeWrapper:
    """Wrapper around Anthropic Claude to provide OpenAI-like interface for agents"""
    
    def __init__(self, api_key: str, model: str = "claude-3-haiku-20240307"):
        """
        Initialize Claude wrapper
        
        Args:
            api_key: Anthropic API key
            model: Claude model to use (default: claude-3-haiku-20240307)
        """
        self.api_key = api_key
        self.model = model
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def chat_completions_create(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.2,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        Create chat completion (OpenAI-compatible interface)
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            
        Returns:
            OpenAI-compatible response format
        """
        try:
            # Convert messages format (Claude uses 'user' and 'assistant' roles)
            # System message needs to be handled separately
            system_message = None
            conversation_messages = []
            
            for msg in messages:
                role = msg.get('role', 'user')
                content = msg.get('content', '')
                
                if role == 'system':
                    system_message = content
                elif role in ['user', 'assistant']:
                    conversation_messages.append({
                        'role': role,
                        'content': content
                    })
            
            # Create Claude message
            response = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_message if system_message else None,
                messages=conversation_messages
            )
            
            # Extract text from response
            response_text = None
            if response.content and len(response.content) > 0:
                if hasattr(response.content[0], 'text'):
                    response_text = response.content[0].text
                elif isinstance(response.content[0], dict) and 'text' in response.content[0]:
                    response_text = response.content[0]['text']
                else:
                    response_text = str(response.content[0])
            
            # Return OpenAI-compatible format
            return {
                'choices': [{
                    'message': {
                        'content': response_text or ''
                    }
                }],
                'model': self.model
            }
            
        except Exception as e:
            error_str = str(e)
            # Check for quota/rate limit errors
            if '429' in error_str or 'rate_limit' in error_str.lower() or 'quota' in error_str.lower():
                print(f"❌ Claude API quota/rate limit exhausted: {error_str[:200]}")
            else:
                print(f"❌ Error calling Claude API: {e}")
                import traceback
                traceback.print_exc()
            
            # Return empty response on error
            return {
                'choices': [{
                    'message': {
                        'content': None
                    }
                }],
                'model': self.model
            }
