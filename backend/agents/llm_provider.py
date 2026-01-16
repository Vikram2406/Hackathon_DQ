"""
LLM Provider Factory - Configurable system for switching between OpenAI and Gemini
"""
import os
from typing import Optional, Dict, Any, List
from enum import Enum


class LLMProvider(str, Enum):
    """Supported LLM providers"""
    OPENAI = "openai"
    GEMINI = "gemini"
    CLAUDE = "claude"


class LLMProviderFactory:
    """Factory for creating LLM clients based on configuration"""
    
    @staticmethod
    def get_provider() -> LLMProvider:
        """Get the configured LLM provider from environment or config"""
        provider = os.getenv('LLM_PROVIDER', 'openai').lower()
        if provider in ['openai', 'gpt']:
            return LLMProvider.OPENAI
        elif provider in ['gemini', 'google']:
            return LLMProvider.GEMINI
        elif provider in ['claude', 'anthropic']:
            return LLMProvider.CLAUDE
        else:
            # Default to OpenAI
            return LLMProvider.OPENAI
    
    @staticmethod
    def create_llm_client(provider: Optional[LLMProvider] = None):
        """
        Create an LLM client based on configured provider
        
        Returns:
            LLM client (OpenAIWrapper or LLMWrapper) with unified interface
        """
        if provider is None:
            provider = LLMProviderFactory.get_provider()
        
        if provider == LLMProvider.OPENAI:
            from agents.openai_wrapper import OpenAIWrapper
            api_key = os.getenv('OPENAI_API_KEY') or os.getenv('OPENAI_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
            return OpenAIWrapper(api_key=api_key)
        
        elif provider == LLMProvider.GEMINI:
            from agents.llm_wrapper import LLMWrapper
            api_key = os.getenv('GOOGLE_API_KEY') or os.getenv('GEMINI_API_KEY')
            if not api_key:
                raise ValueError("Gemini API key not found. Set GOOGLE_API_KEY or GEMINI_API_KEY environment variable.")
            return LLMWrapper(api_key=api_key)
        
        elif provider == LLMProvider.CLAUDE:
            from agents.claude_wrapper import ClaudeWrapper
            api_key = os.getenv('ANTHROPIC_API_KEY') or os.getenv('CLAUDE_API_KEY')
            if not api_key:
                raise ValueError("Claude API key not found. Set ANTHROPIC_API_KEY or CLAUDE_API_KEY environment variable.")
            return ClaudeWrapper(api_key=api_key)
        
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")
    
    @staticmethod
    def create_query_engine(provider: Optional[LLMProvider] = None):
        """
        Create a QueryEngine for chatbot based on configured provider
        
        Returns:
            QueryEngine instance
        """
        if provider is None:
            provider = LLMProviderFactory.get_provider()
        
        if provider == LLMProvider.OPENAI:
            from chatbot.openai_query_engine import OpenAIQueryEngine
            api_key = os.getenv('OPENAI_API_KEY') or os.getenv('OPENAI_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
            return OpenAIQueryEngine(api_key=api_key)
        
        elif provider == LLMProvider.GEMINI:
            from chatbot.query_engine import QueryEngine
            api_key = os.getenv('GOOGLE_API_KEY') or os.getenv('GEMINI_API_KEY')
            if not api_key:
                raise ValueError("Gemini API key not found. Set GOOGLE_API_KEY or GEMINI_API_KEY environment variable.")
            return QueryEngine(api_key=api_key)
        
        elif provider == LLMProvider.CLAUDE:
            # For now, Claude chatbot uses the same wrapper (can be extended later)
            from agents.claude_wrapper import ClaudeWrapper
            api_key = os.getenv('ANTHROPIC_API_KEY') or os.getenv('CLAUDE_API_KEY')
            if not api_key:
                raise ValueError("Claude API key not found. Set ANTHROPIC_API_KEY or CLAUDE_API_KEY environment variable.")
            # Return ClaudeWrapper as query engine (has compatible interface)
            return ClaudeWrapper(api_key=api_key)
        
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")
