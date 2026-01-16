"""
OpenAI Query Engine for chatbot - Natural language questions using OpenAI
"""
import json
from typing import Dict, Any, Optional
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
import os
from datetime import datetime


class OpenAIQueryEngine:
    """Process natural language queries about data quality using OpenAI"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini"):
        """
        Initialize query engine with OpenAI
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI package not installed. Run: pip install openai")
        
        self.api_key = api_key or os.getenv('OPENAI_API_KEY') or os.getenv('OPENAI_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not provided. Set OPENAI_API_KEY environment variable.")
        
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
        """
        Generate AI response using OpenAI
        
        Args:
            query: User's question
            metadata: Quality metrics and metadata
            dataset_name: Name of the dataset
            
        Returns:
            Response text
        """
        try:
            # Build context from metadata
            context_parts = []
            
            if dataset_name:
                context_parts.append(f"Dataset: {dataset_name}")
            
            # Add quality check results
            results = metadata.get('results', {})
            if results:
                context_parts.append("\nQuality Check Results:")
                for check_name, check_result in results.items():
                    status = check_result.get('status', 'UNKNOWN')
                    value = check_result.get('value', 'N/A')
                    context_parts.append(f"  - {check_name}: {status} (value: {value})")
            
            # Add summary
            summary = metadata.get('summary', {})
            if summary:
                context_parts.append(f"\nSummary: {json.dumps(summary, indent=2)}")
            
            # Add agentic issues if available
            agentic_issues = metadata.get('agentic_issues', [])
            if agentic_issues:
                context_parts.append(f"\nAgentic Issues Found: {len(agentic_issues)}")
                # Show sample issues
                for issue in agentic_issues[:5]:
                    issue_type = issue.get('issue_type', 'Unknown')
                    column = issue.get('column', 'Unknown')
                    context_parts.append(f"  - {issue_type} in column '{column}'")
            
            agentic_summary = metadata.get('agentic_summary', {})
            if agentic_summary:
                context_parts.append(f"\nAgentic Summary: {json.dumps(agentic_summary, indent=2)}")
            
            context = "\n".join(context_parts)
            
            # Build prompt
            system_prompt = """You are an expert data quality assistant. You help users understand their data quality issues and provide actionable insights.

You have access to:
- Quality check results (null checks, duplicates, etc.)
- Agentic data quality issues (AI-detected issues like invalid emails, company name variations, etc.)
- Dataset metadata

Provide clear, concise, and helpful answers based on the data provided. If you don't have enough information, say so."""
            
            user_prompt = f"""Context about the dataset:
{context}

User Question: {query}

Please provide a helpful answer based on the data quality information above."""
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                max_tokens=1000
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            return f"I apologize, but I encountered an error processing your question: {str(e)}"
