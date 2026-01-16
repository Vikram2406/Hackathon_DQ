"""
Imputation Agent - Handles Contextual Fill (missing value imputation)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue


class ImputationAgent(BaseAgent):
    """Agent for detecting and suggesting contextual imputation for missing values"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect missing values and suggest contextual imputation
        
        Args:
            dataset_rows: List of row dictionaries
            metadata: Dataset metadata
            llm_client: Optional LLM client
            
        Returns:
            List of AgenticIssue objects
        """
        issues: List[AgenticIssue] = []
        llm = llm_client or self.llm_client
        
        if not dataset_rows or not llm:
            return issues
        
        # Find columns with missing values
        missing_value_indicators = [None, '', 'NULL', 'N/A', 'NA', 'null', 'none']
        
        for row_idx, row in enumerate(dataset_rows):
            for col, value in row.items():
                if value in missing_value_indicators or (isinstance(value, str) and value.strip() in ['', 'NULL', 'N/A', 'NA', 'null', 'none']):
                    # Try to impute based on context
                    try:
                        imputation = self._llm_impute_value(row, col, llm)
                        if imputation:
                            suggested, confidence, explanation = imputation
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=col,
                                issue_type="ContextualFill",
                                dirty_value=value or 'NULL',
                                suggested_value=suggested,
                                confidence=confidence,
                                explanation=explanation,
                                why_agentic="Uses product/row context to 'guess' the missing attribute"
                            ))
                    except Exception as e:
                        print(f"Error in LLM imputation: {e}")
        
        return issues
    
    def _llm_impute_value(self, row: Dict[str, Any], missing_column: str, llm) -> Optional[tuple]:
        """Use LLM to impute missing value based on row context"""
        try:
            # Build context from other columns
            context = {k: v for k, v in row.items() if k != missing_column and v not in [None, '', 'NULL']}
            
            prompt = f"""Given this row data, suggest a value for the missing column '{missing_column}':

Row context: {context}

Return ONLY a JSON object with:
{{
    "imputed": "suggested_value",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation of why this value makes sense"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a data imputation assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=150
            )
            
            import json
            result = json.loads(content)
            return (
                result.get('imputed'),
                result.get('confidence', 0.6),
                result.get('explanation', 'Context-based imputation')
            )
        except Exception:
            return None