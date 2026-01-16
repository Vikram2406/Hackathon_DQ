"""
Units Agent - Handles Scale Mismatch (unit normalization)
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
import re
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue
from utils.data_cleaning import parse_units, convert_units


class UnitsAgent(BaseAgent):
    """Agent for detecting and fixing unit scale mismatches"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect unit scale mismatches
        
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
        
        # Detect columns that might contain measurements (heuristic)
        measurement_columns = []
        for col in dataset_rows[0].keys():
            col_lower = col.lower()
            if any(kw in col_lower for kw in ['height', 'weight', 'length', 'width', 'distance', 'size', 'measurement']):
                measurement_columns.append(col)
        
        # Determine canonical unit per column - standardize ALL to same unit
        # For now, default to cm for length/height, kg for weight
        canonical_units = {}
        for col in measurement_columns:
            col_lower = col.lower()
            # Standardize all values in the same column to ONE unit
            if 'weight' in col_lower:
                canonical_units[col] = 'kg'  # All weights to kg
            elif 'height' in col_lower or 'length' in col_lower or 'distance' in col_lower:
                canonical_units[col] = 'cm'  # All heights/lengths to cm
            else:
                # Default to cm for other measurements
                canonical_units[col] = 'cm'
        
        # Find the most common unit in each column to use as canonical
        # This is the AI-powered part: we analyze the data to determine the best unit
        for col in measurement_columns:
            units_found = {}
            for row in dataset_rows[:1000]:  # Check more rows
                value = row.get(col)
                if value and isinstance(value, str):
                    parsed = parse_units(str(value))
                    if parsed:
                        _, unit, _ = parsed
                        units_found[unit] = units_found.get(unit, 0) + 1
            
            # If we found units, use the most common one as canonical
            if units_found:
                most_common_unit = max(units_found.items(), key=lambda x: x[1])[0]
                count = units_found[most_common_unit]
                total = sum(units_found.values())
                canonical_units[col] = most_common_unit
                print(f"DEBUG: UnitsAgent - Column '{col}': Most common unit is '{most_common_unit}' ({count}/{total} = {count/total*100:.1f}% of values). Will standardize all values to {most_common_unit}.")
                print(f"DEBUG: UnitsAgent - Unit distribution for '{col}': {units_found}")
        
        # Process each row - flag ALL values that don't match canonical unit
        for row_idx, row in enumerate(dataset_rows):
            for col in measurement_columns:
                value = row.get(col)
                if value and isinstance(value, str) and value.strip():
                    parsed = parse_units(str(value))
                    if parsed:
                        numeric_value, unit, confidence = parsed
                        canonical_unit = canonical_units.get(col, 'cm')
                        
                        # Flag ALL values that don't match the canonical unit
                        if unit != canonical_unit:
                            # Convert to canonical unit
                            converted = convert_units(numeric_value, unit, canonical_unit)
                            if converted is not None:
                                suggested = f"{converted:.2f} {canonical_unit}"
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=col,
                                    issue_type="ScaleMismatch",
                                    dirty_value=value,
                                    suggested_value=suggested,
                                    confidence=confidence,
                                    explanation=f"Unit mismatch: '{value}' uses {unit}. Standardizing to {canonical_unit} (most common unit in this column: {canonical_unit})",
                                    why_agentic=f"🤖 AI-Powered: Analyzes all values in column to find most common unit ({canonical_unit}), then standardizes all values to that unit for consistency"
                                ))
                    elif value and isinstance(value, str) and value.strip() and not value.replace('.', '').replace('-', '').isdigit():
                        # If it looks like a measurement but couldn't parse, still flag it
                        # (might be a complex format that needs LLM)
                        if llm:
                            try:
                                llm_result = self._llm_normalize_units(str(value), llm)
                                if llm_result:
                                    suggested, confidence, explanation = llm_result
                                    issues.append(self._create_issue(
                                        row_id=row_idx,
                                        column=col,
                                        issue_type="ScaleMismatch",
                                        dirty_value=value,
                                        suggested_value=suggested,
                                        confidence=confidence,
                                        explanation=explanation,
                                        why_agentic="LLM understands context and can parse complex unit expressions"
                                    ))
                            except Exception as e:
                                print(f"Error in LLM unit normalization: {e}")
                    elif llm:
                        # Use LLM for complex unit strings
                        try:
                            llm_result = self._llm_normalize_units(str(value), llm)
                            if llm_result:
                                suggested, confidence, explanation = llm_result
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=col,
                                    issue_type="ScaleMismatch",
                                    dirty_value=value,
                                    suggested_value=suggested,
                                    confidence=confidence,
                                    explanation=explanation,
                                    why_agentic="LLM understands context and can parse complex unit expressions"
                                ))
                        except Exception as e:
                            print(f"Error in LLM unit normalization: {e}")
        
        return issues
    
    def _llm_normalize_units(self, value_string: str, llm) -> Optional[tuple]:
        """Use LLM to normalize units"""
        try:
            prompt = f"""Normalize this measurement to a standard unit (prefer cm for length, kg for weight): "{value_string}"

Return ONLY a JSON object with:
{{
    "normalized": "123.45 cm",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a unit normalization assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            if not content:
                return None
            
            import json
            result = json.loads(content)
            return (
                result.get('normalized'),
                result.get('confidence', 0.7),
                result.get('explanation', 'LLM-normalized unit')
            )
        except Exception:
            return None