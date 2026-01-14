"""
AI-powered explanation generation using LLM
"""
import json
from typing import Dict, Any, Optional
import os
from openai import OpenAI


class AIExplainer:
    """Generate human-readable explanations using LLM"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        """
        Initialize AI explainer
        
        Args:
            api_key: OpenAI API key (if None, uses environment variable)
            model: Model to use (default: gpt-4)
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not provided")
        
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
    
    def explain_failure(
        self,
        check_results: Dict[str, Any],
        historical_context: Optional[Dict[str, Any]] = None,
        dataset_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate explanation for quality check failures
        
        Args:
            check_results: Results from quality checks
            historical_context: Historical metrics for comparison
            dataset_info: Information about the dataset
        
        Returns:
            Dictionary with AI-generated explanation
        """
        prompt = self._build_failure_prompt(check_results, historical_context, dataset_info)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data quality expert explaining issues to data engineers. Provide clear, actionable insights in plain English."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=800
            )
            
            explanation_text = response.choices[0].message.content
            
            # Parse structured response
            parsed = self._parse_explanation(explanation_text)
            
            return {
                'success': True,
                'explanation': explanation_text,
                'root_cause': parsed.get('root_cause'),
                'business_impact': parsed.get('business_impact'),
                'recommended_actions': parsed.get('recommended_actions', []),
                'risk_level': parsed.get('risk_level', 'MEDIUM'),
                'model_used': self.model
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'explanation': f"Failed to generate AI explanation: {str(e)}"
            }
    
    def _build_failure_prompt(
        self,
        check_results: Dict[str, Any],
        historical_context: Optional[Dict[str, Any]],
        dataset_info: Optional[Dict[str, Any]]
    ) -> str:
        """Build prompt for failure explanation"""
        
        prompt = f"""Data quality check failed with the following results:

{json.dumps(check_results, indent=2)}
"""
        
        if historical_context:
            prompt += f"""
Historical context (last 7 days):
{json.dumps(historical_context, indent=2)}
"""
        
        if dataset_info:
            prompt += f"""
Dataset information:
{json.dumps(dataset_info, indent=2)}
"""
        
        prompt += """
Provide a concise analysis with:

1. **Root Cause** (1-2 sentences): Why did this failure occur?
2. **Business Impact** (2-3 sentences): What are the consequences?
3. **Recommended Actions** (3-5 bullet points): Specific steps to fix this
4. **Risk Level** (LOW/MEDIUM/HIGH/CRITICAL): Overall risk assessment

Use plain English. Be specific and actionable. Focus on practical solutions.
"""
        
        return prompt
    
    def _parse_explanation(self, text: str) -> Dict[str, Any]:
        """Parse structured information from explanation text"""
        parsed = {}
        
        # Extract risk level
        risk_keywords = {
            'CRITICAL': ['critical', 'severe', 'urgent'],
            'HIGH': ['high risk', 'significant', 'major'],
            'MEDIUM': ['medium', 'moderate'],
            'LOW': ['low', 'minor', 'minimal']
        }
        
        text_lower = text.lower()
        for level, keywords in risk_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                parsed['risk_level'] = level
                break
        
        if 'risk_level' not in parsed:
            parsed['risk_level'] = 'MEDIUM'
        
        # Try to extract sections
        sections = text.split('\n\n')
        for section in sections:
            if 'root cause' in section.lower():
                parsed['root_cause'] = section.split(':', 1)[1].strip() if ':' in section else section
            elif 'business impact' in section.lower():
                parsed['business_impact'] = section.split(':', 1)[1].strip() if ':' in section else section
            elif 'recommended actions' in section.lower() or 'recommendations' in section.lower():
                # Extract bullet points
                lines = section.split('\n')
                actions = [line.strip('- ').strip() for line in lines if line.strip().startswith('-') or line.strip().startswith('•')]
                parsed['recommended_actions'] = actions
        
        return parsed
    
    def generate_summary(
        self,
        all_results: Dict[str, Any],
        dataset_name: str
    ) -> str:
        """
        Generate overall summary of data quality results
        
        Args:
            all_results: All quality check results
            dataset_name: Name of the dataset
        
        Returns:
            Summary text
        """
        prompt = f"""Generate a brief executive summary for data quality validation of dataset: {dataset_name}

Results:
{json.dumps(all_results, indent=2)}

Provide a 2-3 sentence summary highlighting:
- Overall status (PASS/FAIL)
- Key issues found (if any)
- Recommended next steps

Keep it concise and actionable.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a data quality expert providing executive summaries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Failed to generate summary: {str(e)}"
