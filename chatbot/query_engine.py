"""
Chatbot query engine for natural language questions
"""
import json
from typing import Dict, Any, Optional, List
from openai import OpenAI
import os
from datetime import datetime


class QueryEngine:
    """Process natural language queries about data quality"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        """
        Initialize query engine
        
        Args:
            api_key: OpenAI API key
            model: Model to use
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not provided")
        
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
    
    def process_query(
        self,
        query: str,
        metadata: Dict[str, Any],
        dataset_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process natural language query
        
        Args:
            query: User's question
            metadata: Quality metrics and metadata (NOT raw data)
            dataset_name: Name of the dataset
        
        Returns:
            Dictionary with response and metadata
        """
        # Classify intent
        intent = self._classify_intent(query)
        
        # Generate response
        response_text = self._generate_response(query, metadata, dataset_name)
        
        return {
            'query': query,
            'response': response_text,
            'intent': intent,
            'dataset_name': dataset_name,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _classify_intent(self, query: str) -> str:
        """Classify query intent"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['where', 'which', 'location', 'column']):
            return 'location_query'
        elif any(word in query_lower for word in ['normal', 'anomaly', 'unusual', 'strange']):
            return 'anomaly_check'
        elif any(word in query_lower for word in ['yesterday', 'last', 'previous', 'compare', 'changed']):
            return 'comparison'
        elif any(word in query_lower for word in ['should', 'block', 'proceed', 'recommend', 'action']):
            return 'action_recommendation'
        elif any(word in query_lower for word in ['trend', 'pattern', 'over time']):
            return 'trend_analysis'
        else:
            return 'general_query'
    
    def _generate_response(
        self,
        query: str,
        metadata: Dict[str, Any],
        dataset_name: Optional[str]
    ) -> str:
        """Generate response using LLM"""
        
        prompt = f"""You are a data quality assistant helping engineers understand their data quality metrics.

Dataset: {dataset_name or 'Unknown'}

User Question: {query}

Available Quality Metrics:
{json.dumps(metadata, indent=2)}

Provide a clear, concise answer in plain English. Focus on:
- Answering the specific question asked
- Using data from the metrics provided
- Being actionable and specific
- Avoiding technical jargon when possible

If the question asks for recommendations, be decisive and clear about what action to take.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful data quality assistant. Provide clear, actionable answers based on the metrics provided. Never make up data."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"I apologize, but I encountered an error processing your question: {str(e)}"
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """
        Validate query for safety
        
        Args:
            query: User's question
        
        Returns:
            Dictionary with validation results
        """
        import re
        
        # Block dangerous patterns
        blocked_patterns = [
            r'SELECT.*FROM',
            r'DELETE',
            r'UPDATE',
            r'DROP',
            r'INSERT',
            r'exec',
            r'eval',
            r'__import__',
            r'subprocess',
        ]
        
        for pattern in blocked_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return {
                    'is_valid': False,
                    'reason': 'Query contains unsafe operations'
                }
        
        # Check length
        if len(query) > 1000:
            return {
                'is_valid': False,
                'reason': 'Query too long (max 1000 characters)'
            }
        
        return {
            'is_valid': True
        }


class ChatbotService:
    """High-level chatbot service"""
    
    def __init__(self, query_engine: QueryEngine):
        """
        Initialize chatbot service
        
        Args:
            query_engine: QueryEngine instance
        """
        self.query_engine = query_engine
        self.conversation_history: List[Dict[str, Any]] = []
    
    def ask(
        self,
        query: str,
        metadata: Dict[str, Any],
        dataset_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Ask a question
        
        Args:
            query: User's question
            metadata: Quality metrics
            dataset_name: Dataset name
        
        Returns:
            Response dictionary
        """
        # Validate query
        validation = self.query_engine.validate_query(query)
        if not validation['is_valid']:
            return {
                'success': False,
                'error': validation['reason'],
                'query': query
            }
        
        # Process query
        result = self.query_engine.process_query(query, metadata, dataset_name)
        
        # Store in history
        self.conversation_history.append(result)
        
        return {
            'success': True,
            **result
        }
    
    def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get conversation history"""
        return self.conversation_history[-limit:]
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
