"""
Chatbot Package - Supports both OpenAI and Gemini via LLMProviderFactory
"""
from typing import Dict, Any, Optional
from agents.llm_provider import LLMProviderFactory

# Dynamic import based on provider
def get_query_engine():
    """Get QueryEngine based on configured provider"""
    return LLMProviderFactory.create_query_engine()

# For backward compatibility - try to import QueryEngine
try:
    from chatbot.query_engine import QueryEngine
except ImportError:
    try:
        from chatbot.openai_query_engine import OpenAIQueryEngine as QueryEngine
    except ImportError:
        QueryEngine = None

# Backward compatibility wrapper
class ChatbotService:
    """Wrapper for backward compatibility"""
    def __init__(self, query_engine):
        self.query_engine = query_engine
    
    def process_query(self, query: str, metadata: Dict[str, Any], dataset_name: Optional[str] = None):
        return self.query_engine.process_query(query, metadata, dataset_name)

__all__ = [
    'QueryEngine',
    'ChatbotService',
    'get_query_engine',
]
