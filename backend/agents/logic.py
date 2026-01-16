"""
Logic Agent - Handles Cross-Field Conflicts and Temporal Paradoxes
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue
from utils.data_cleaning import parse_date


class LogicAgent(BaseAgent):
    """Agent for detecting cross-field logical inconsistencies and temporal paradoxes"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect cross-field conflicts and temporal paradoxes
        
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
        
        # Data-driven: Analyze columns to find date columns from actual data patterns
        from agents.data_analyzer import DataAnalyzer
        column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)
        relationships = DataAnalyzer.find_related_columns(dataset_rows, 'date')
        
        # Find date columns: by name OR by detected type
        date_columns = []
        for col, analysis in column_analysis.items():
            col_lower = col.lower()
            if (any(kw in col_lower for kw in ['date', 'time', 'created', 'updated', 'timestamp', 'dob', 'birth', 'start', 'end']) or
                analysis.get('type') == 'date'):
                date_columns.append(col)
        
        # Find location columns
        location_columns = {'city': None, 'state': None, 'country': None}
        for col in dataset_rows[0].keys():
            col_lower = col.lower()
            if 'city' in col_lower:
                location_columns['city'] = col
            elif 'state' in col_lower or 'province' in col_lower:
                location_columns['state'] = col
            elif 'country' in col_lower:
                location_columns['country'] = col
        
        # Find birth date and job start date columns - data-driven approach
        birth_date_col = None
        job_start_col = None
        
        # Use LLM to intelligently identify date column relationships
        if llm and len(date_columns) >= 2:
            date_cols_str = ', '.join(date_columns)
            sample_data = {col: [str(row.get(col, ''))[:50] for row in dataset_rows[:5] if row.get(col)] for col in date_columns}
            
            try:
                prompt = f"""Analyze these date columns and identify which ones are likely:
- Birth date / Date of birth
- Job start date / Hire date / Employment start date

Date columns: {date_cols_str}
Sample data: {json.dumps(sample_data)}

Return ONLY a JSON object:
{{
    "birth_date_column": "column_name or null",
    "job_start_column": "column_name or null"
}}"""
                
                from agents.llm_helper import call_llm
                content = call_llm(
                    llm,
                    messages=[
                        {"role": "system", "content": "You are a data analysis assistant. Analyze column names and sample data to identify date column relationships. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=100
                )
                
                if content:
                    result = json.loads(content)
                    birth_date_col = result.get('birth_date_column')
                    job_start_col = result.get('job_start_column')
            except Exception as e:
                print(f"Error in LLM date column detection: {e}")
        
        # Fallback to name-based detection if LLM didn't work
        if not birth_date_col or not job_start_col:
            for col in dataset_rows[0].keys():
                col_lower = col.lower()
                if 'birth' in col_lower or 'dob' in col_lower:
                    birth_date_col = col
                if ('job' in col_lower or 'start' in col_lower or 'hire' in col_lower) and 'birth' not in col_lower:
                    job_start_col = col
        
        # Process each row
        for row_idx, row in enumerate(dataset_rows):
            # Specific check: Job start date must be after birth date
            if birth_date_col and job_start_col:
                birth_value = row.get(birth_date_col)
                job_start_value = row.get(job_start_col)
                
                if birth_value and job_start_value:
                    birth_parsed = parse_date(str(birth_value))
                    job_parsed = parse_date(str(job_start_value))
                    
                    if birth_parsed and job_parsed:
                        birth_date = birth_parsed[0]  # ISO string
                        job_date = job_parsed[0]
                        
                        # If job start is before birth date, it's impossible - set to null
                        if job_date < birth_date:
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=job_start_col,
                                issue_type="TemporalParadox",
                                dirty_value=job_start_value,
                                suggested_value=None,  # Set to null - impossible date
                                confidence=0.95,
                                explanation=f"Job start date ({job_date}) is before birth date ({birth_date}). This is impossible - should be null.",
                                why_agentic="AI detects logical impossibilities: job cannot start before birth"
                            ))
            
            # Temporal Paradox Detection for other date pairs
            if len(date_columns) >= 2:
                dates = {}
                for col in date_columns:
                    value = row.get(col)
                    if value:
                        parsed = parse_date(str(value))
                        if parsed:
                            dates[col] = parsed[0]  # ISO date string
                
                # Check for temporal paradoxes (e.g., end_date < start_date)
                date_list = list(dates.items())
                for i, (col1, date1) in enumerate(date_list):
                    for col2, date2 in date_list[i+1:]:
                        if date1 > date2:
                            # Potential paradox
                            col1_lower = col1.lower()
                            col2_lower = col2.lower()
                            
                            # Skip if we already handled birth/job start above
                            if (birth_date_col and job_start_col and 
                                (col1 == birth_date_col or col1 == job_start_col) and
                                (col2 == birth_date_col or col2 == job_start_col)):
                                continue
                            
                            # Check if this is a known paradox pattern
                            if ('start' in col1_lower and 'end' in col2_lower) or \
                               ('created' in col1_lower and 'updated' in col2_lower):
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=col1,
                                    issue_type="TemporalParadox",
                                    dirty_value=f"{col1}: {date1}, {col2}: {date2}",
                                    suggested_value=None,  # Mark for manual review
                                    confidence=0.9,
                                    explanation=f"Temporal inconsistency: {col1} ({date1}) is after {col2} ({date2})",
                                    why_agentic="AI detects logical impossibilities between date columns"
                                ))
            
            # Cross-Field Conflict Detection (e.g., City/State mismatch)
            if location_columns['city'] and location_columns['state']:
                city = row.get(location_columns['city'])
                state = row.get(location_columns['state'])
                
                if city and state and llm:
                    # Use LLM to check if city/state combination is valid
                    try:
                        is_valid = self._llm_validate_location(city, state, llm)
                        if not is_valid:
                            # Use AI to find the correct state for this city
                            correct_state = None
                            if llm:
                                try:
                                    from agents.geographic_enrichment import GeographicEnrichmentAgent
                                    geo_agent = GeographicEnrichmentAgent(llm_client=llm)
                                    correct_state = geo_agent._find_state_from_city(city, None, llm)
                                    if correct_state:
                                        print(f"DEBUG: LogicAgent - Found correct state '{correct_state}' for city '{city}'")
                                except Exception as e:
                                    print(f"DEBUG: LogicAgent - Error finding state: {e}")
                            
                            suggested_state = correct_state if correct_state else f"[AI failed - verify state for {city}]"
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=location_columns['state'],
                                issue_type="CrossFieldConflict",
                                dirty_value=state,
                                suggested_value=suggested_state,
                                confidence=0.85 if correct_state else 0.60,
                                explanation=f"Geographic inconsistency: {city} is not in {state}. Correct state should be '{suggested_state}'",
                                why_agentic="🤖 AI-Generated (Claude): Uses AI to determine correct state for city, then detects geographic inconsistencies"
                            ))
                    except Exception as e:
                        print(f"Error in LLM location validation: {e}")
        
        return issues
    
    def _llm_validate_location(self, city: str, state: str, llm) -> bool:
        """Use LLM to validate city/state combination"""
        try:
            prompt = f"""Is this city/state combination valid? City: {city}, State: {state}

Return ONLY a JSON object with:
{{
    "valid": true/false,
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a geography validation assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            if not content:
                return True  # Default to valid if LLM fails
            
            import json
            result = json.loads(content)
            return result.get('valid', True)
        except Exception:
            return True  # Default to valid if LLM fails