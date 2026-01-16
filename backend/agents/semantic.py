"""
Semantic Agent - Handles Entity Resolution
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from collections import defaultdict
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue
from utils.data_cleaning import fuzzy_match_category


class SemanticAgent(BaseAgent):
    """Agent for detecting and resolving entity variations (same entity, different names)"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect entity resolution issues (same entity, different representations)
        
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
        
        # Detect entity columns (company names, organization names, etc.)
        # CRITICAL: DO NOT include 'name' - personal names should NOT be modified!
        entity_columns = []
        for col in dataset_rows[0].keys():
            col_lower = col.lower()
            
            # Exclude personal name columns
            if any(kw in col_lower for kw in ['firstname', 'first_name', 'lastname', 'last_name', 
                                               'fullname', 'full_name', 'username', 'user_name',
                                               'person', 'customer', 'employee', 'contact']):
                continue  # Skip personal name columns
            
            # Only detect actual entity columns (companies, organizations, brands)
            if any(kw in col_lower for kw in ['company', 'organization', 'org', 'entity', 'brand', 'vendor', 'supplier']):
                entity_columns.append(col)
                print(f"DEBUG: SemanticAgent - Detected entity column: '{col}'")
        
        # Group similar entities
        for col in entity_columns:
            # Collect all unique values
            unique_values = set()
            value_to_rows = defaultdict(list)
            
            for row_idx, row in enumerate(dataset_rows):
                value = row.get(col)
                if value and isinstance(value, str):
                    value_clean = value.strip()
                    unique_values.add(value_clean)
                    value_to_rows[value_clean].append(row_idx)
            
            # For each value, check if it's similar to others (entity resolution)
            processed = set()
            for value in unique_values:
                if value in processed:
                    continue
                
                # Find similar values
                similar = [v for v in unique_values if v != value and self._are_similar_entities(value, v)]
                
                if similar:
                    # Use LLM to determine canonical name
                    try:
                        canonical = self._llm_resolve_entity([value] + similar, llm)
                        if canonical:
                            # Mark all similar values for standardization
                            for similar_val in [value] + similar:
                                if similar_val != canonical:
                                    for row_idx in value_to_rows[similar_val]:
                                        issues.append(self._create_issue(
                                            row_id=row_idx,
                                            column=col,
                                            issue_type="EntityResolution",
                                            dirty_value=similar_val,
                                            suggested_value=canonical,
                                            confidence=0.8,
                                            explanation=f"Entity variation: '{similar_val}' refers to the same entity as '{canonical}'",
                                            why_agentic="Understands these all refer to the same real-world entity"
                                        ))
                                    processed.add(similar_val)
                    except Exception as e:
                        print(f"Error in LLM entity resolution: {e}")
        
        return issues
    
    def _are_similar_entities(self, val1: str, val2: str) -> bool:
        """Check if two values might refer to the same entity"""
        val1_lower = val1.lower()
        val2_lower = val2.lower()
        
        # Exact match (case-insensitive)
        if val1_lower == val2_lower:
            return True
        
        # One contains the other
        if val1_lower in val2_lower or val2_lower in val1_lower:
            return True
        
        # Fuzzy similarity
        from utils.data_cleaning import _simple_similarity
        similarity = _simple_similarity(val1_lower, val2_lower)
        return similarity > 0.7
    
    def _llm_resolve_entity(self, variants: List[str], llm) -> Optional[str]:
        """Use LLM to determine canonical entity name"""
        try:
            prompt = f"""These values likely refer to the same entity. Return the canonical/standard name:

Variants: {', '.join(variants)}

Return ONLY a JSON object with:
{{
    "canonical": "standard_name",
    "confidence": 0.0-1.0
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are an entity resolution assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            import json
            result = json.loads(content)
            return result.get('canonical')
        except Exception:
            return None