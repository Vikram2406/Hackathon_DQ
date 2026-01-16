"""
Categorical Agent - Handles Fuzzy Mapping (typo correction)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from collections import Counter
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue
from utils.data_cleaning import fuzzy_match_category


class CategoricalAgent(BaseAgent):
    """Agent for detecting and fixing categorical value typos/variations"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect categorical value mismatches (typos, variations)
        
        Args:
            dataset_rows: List of row dictionaries
            metadata: Dataset metadata
            llm_client: Optional LLM client
            
        Returns:
            List of AgenticIssue objects
        """
        issues: List[AgenticIssue] = []
        llm = llm_client or self.llm_client
        
        if not dataset_rows:
            return issues
        
        # Detect categorical columns (columns with limited unique values)
        categorical_columns = []
        for col in dataset_rows[0].keys():
            # Check if column has limited unique values (likely categorical)
            unique_values = set()
            for row in dataset_rows[:1000]:  # Sample
                val = row.get(col)
                if val:
                    unique_values.add(str(val).strip().lower())
            
            # If less than 50 unique values, treat as categorical
            if len(unique_values) < 50 and len(unique_values) > 1:
                categorical_columns.append(col)
        
        # For each categorical column, determine allowed values (most common)
        for col in categorical_columns:
            value_counts = Counter()
            for row in dataset_rows:
                val = row.get(col)
                if val:
                    value_counts[str(val).strip()] += 1
            
            # Top values are "allowed" (threshold: appear at least 2% of the time)
            total = sum(value_counts.values())
            threshold = max(2, total * 0.02)
            allowed_values = [val for val, count in value_counts.most_common() if count >= threshold]
            
            if len(allowed_values) < 2:
                continue  # Not enough variation
            
            # Process each row
            for row_idx, row in enumerate(dataset_rows):
                value = row.get(col)
                if value:
                    value_str = str(value).strip()
                    
                    # Check if value is in allowed list (case-insensitive)
                    if value_str.lower() not in [v.lower() for v in allowed_values]:
                        # Try fuzzy match
                        matched = fuzzy_match_category(value_str, allowed_values, threshold=0.6)
                        if matched:
                            suggested, confidence = matched
                            if suggested != value_str:
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=col,
                                    issue_type="FuzzyMapping",
                                    dirty_value=value_str,
                                    suggested_value=suggested,
                                    confidence=confidence,
                                    explanation=f"Typo/variation detected: '{value_str}' should be '{suggested}'",
                                    why_agentic="Fixes typos without needing a manual lookup table"
                                ))
                        elif llm:
                            # Use LLM for complex cases
                            try:
                                llm_result = self._llm_map_category(value_str, allowed_values, llm)
                                if llm_result:
                                    suggested, confidence, explanation = llm_result
                                    issues.append(self._create_issue(
                                        row_id=row_idx,
                                        column=col,
                                        issue_type="FuzzyMapping",
                                        dirty_value=value_str,
                                        suggested_value=suggested,
                                        confidence=confidence,
                                        explanation=explanation,
                                        why_agentic="LLM understands context and can map variations to correct categories"
                                    ))
                            except Exception as e:
                                print(f"Error in LLM category mapping: {e}")
        
        return issues
    
    def _llm_map_category(self, value: str, allowed_categories: List[str], llm) -> Optional[tuple]:
        """Use LLM to map a value to allowed categories"""
        try:
            prompt = f"""Map this value to one of the allowed categories: "{value}"

Allowed categories: {', '.join(allowed_categories)}

Return ONLY a JSON object with:
{{
    "mapped": "category_name",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a categorical mapping assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            import json
            result = json.loads(content)
            mapped = result.get('mapped')
            if mapped in allowed_categories:
                return (
                    mapped,
                    result.get('confidence', 0.7),
                    result.get('explanation', 'LLM-mapped category')
                )
        except Exception:
            pass
        return None