"""
Geographic Enrichment Agent - Finds missing country information based on city names
"""
import sys
import os
import json
import math
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue


class GeographicEnrichmentAgent(BaseAgent):
    """Agent for enriching geographic data (finding missing countries from cities)"""
    
    def __init__(self, llm_client=None):
        super().__init__(llm_client)
        self.city_country_cache = {}  # Cache city -> country/state mappings
        # Override category name for better display
        self.category = "GeographicEnrichment"
    
    def _is_null_or_empty(self, value: Any) -> bool:
        """Check if value is null, empty, or represents a missing value"""
        if value is None:
            return True
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return True
        if isinstance(value, str):
            value_lower = value.strip().lower()
            return value_lower in ['', 'null', 'none', 'n/a', 'na', 'nan', 'nil', 'undefined', 'missing']
        return False
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect missing country information and suggest based on city names
        
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
            print("DEBUG: GeographicEnrichmentAgent: No dataset rows provided")
            return issues
        
        print(f"DEBUG: GeographicEnrichmentAgent: Processing {len(dataset_rows)} rows")
        
        # Collect ALL unique column names across all rows (handle case variations)
        all_columns = set()
        for row in dataset_rows:
            all_columns.update(row.keys())
        
        print(f"DEBUG: GeographicEnrichmentAgent: All unique columns: {list(all_columns)[:10]}")
        
        # Find city, state, and country columns (more robust detection across all columns)
        city_columns = []
        state_columns = []
        country_columns = []
        
        # Create a mapping of normalized column names to actual column names
        col_normalized_to_actual = {}
        for col in all_columns:
            col_lower = col.lower().strip()
            col_normalized_to_actual[col_lower] = col
            
            # City detection (more keywords)
            if any(kw in col_lower for kw in ['city', 'town', 'location', 'place']):
                city_columns.append(col)
            # State detection (more keywords and variations)
            if any(kw in col_lower for kw in ['state', 'province', 'region', 'territory', 'district', 'county']):
                state_columns.append(col)
            # Country detection (more keywords)
            if any(kw in col_lower for kw in ['country', 'nation', 'nationality']):
                country_columns.append(col)
        
        # If we found columns with different cases, prefer the most common one
        # For now, just use the first one found, but normalize access
        def normalize_column_access(row, columns_list):
            """Get value from row using any column name variation"""
            if not columns_list:
                return None, None
            # Try exact match first
            for col in columns_list:
                if col in row:
                    return row.get(col), col
            # Try case-insensitive match
            row_lower = {k.lower(): (k, v) for k, v in row.items()}
            for col in columns_list:
                col_lower = col.lower()
                if col_lower in row_lower:
                    actual_col, value = row_lower[col_lower]
                    return value, actual_col
            return None, None
        
        # Debug: Print detected columns
        print(f"DEBUG: GeographicEnrichmentAgent: Detected city columns: {city_columns}")
        print(f"DEBUG: GeographicEnrichmentAgent: Detected state columns: {state_columns}")
        print(f"DEBUG: GeographicEnrichmentAgent: Detected country columns: {country_columns}")
        
        if not city_columns:
            print("DEBUG: GeographicEnrichmentAgent: No city columns found - cannot proceed")
            return issues
        
        if not llm:
            print("DEBUG: GeographicEnrichmentAgent: No LLM client available - cannot use AI")
            return issues
        
        # Process each row
        for row_idx, row in enumerate(dataset_rows):
            # Find city value (handle case variations)
            city_value = None
            city_col = None
            for col in city_columns:
                # Try exact match first
                if col in row:
                    value = row.get(col)
                    if value and isinstance(value, str) and value.strip():
                        city_value = value.strip()
                        city_col = col
                        break
                else:
                    # Try case-insensitive match
                    row_lower = {k.lower(): (k, v) for k, v in row.items()}
                    col_lower = col.lower()
                    if col_lower in row_lower:
                        actual_col, value = row_lower[col_lower]
                        if value and isinstance(value, str) and value.strip():
                            city_value = value.strip()
                            city_col = actual_col
                            break
            
            if not city_value:
                continue
            
            # Find state value (handle None, NaN, empty strings, and strip numeric prefixes)
            state_value = None
            state_col = None
            for col in state_columns:
                # Try exact match first
                if col in row:
                    value = row.get(col)
                else:
                    # Try case-insensitive match
                    row_lower = {k.lower(): (k, v) for k, v in row.items()}
                    col_lower = col.lower()
                    if col_lower in row_lower:
                        actual_col, value = row_lower[col_lower]
                        col = actual_col  # Use actual column name
                    else:
                        value = None
                
                # Always extract state value, even if it seems wrong (we'll validate it)
                if value is not None:
                    if isinstance(value, str):
                        state_value = value.strip()
                    else:
                        state_value = str(value).strip()
                    # Remove numeric prefixes (e.g., "0 Washington" -> "Washington")
                    state_value = re.sub(r'^\d+\s+', '', state_value).strip()
                    # Only set state_col if we have a non-empty value
                    if state_value:
                        state_col = col
                        break
                else:
                    # Value is None, but column exists - this is a missing state
                    state_col = col
                    break
            
            # Find country value (handle None, NaN, empty strings)
            country_value = None
            country_col = None
            for col in country_columns:
                # Try exact match first
                if col in row:
                    value = row.get(col)
                else:
                    # Try case-insensitive match
                    row_lower = {k.lower(): (k, v) for k, v in row.items()}
                    col_lower = col.lower()
                    if col_lower in row_lower:
                        actual_col, value = row_lower[col_lower]
                        col = actual_col  # Use actual column name
                    else:
                        value = None
                
                if not self._is_null_or_empty(value):
                    if isinstance(value, str):
                        country_value = value.strip()
                    else:
                        country_value = str(value).strip()
                    country_col = col
                    break
            
            # NEW LOGIC: First find state from city, then find country from state
            # This ensures consistency: City -> State -> Country
            if city_value and llm:
                # Step 1: Find the correct STATE for this city (this is the primary lookup)
                # Ensure we have a state column (use first one if we found state_columns, or create one)
                if not state_col:
                    state_col = state_columns[0] if state_columns else None
                
                # CRITICAL: Always proceed if we have a city - we need to detect state issues
                # Even if state column doesn't exist in data, we should still find the correct state
                # and create issues if the state is wrong or missing
                if state_col or state_columns:  # Proceed if state column exists OR if we detected state columns
                    if not state_col:
                        state_col = state_columns[0]  # Use first detected state column
                    # Use AI to find correct state for this city
                    cache_key_state = f"{city_value}_state"
                    if cache_key_state in self.city_country_cache:
                        correct_state = self.city_country_cache[cache_key_state]
                    else:
                        # Use AI to find state from city (primary lookup)
                        print(f"DEBUG: Step 1 - Finding state for city '{city_value}'")
                        correct_state = self._find_state_from_city(city_value, country_value, llm)
                        if correct_state:
                            self.city_country_cache[cache_key_state] = correct_state
                            print(f"DEBUG: ✅ AI returned state '{correct_state}' for city '{city_value}'")
                        else:
                            print(f"DEBUG: ⚠️ AI returned None for state of city '{city_value}'")
                    
                    # Step 2: ALWAYS validate state and country, even if AI failed to find correct values
                    # We can still flag issues as "state is wrong/missing" even without AI suggestion
                    
                    # First, check if state is missing or wrong (even without AI suggestion)
                    if self._is_null_or_empty(state_value):
                        # State is missing
                        print(f"DEBUG: ⚠️ DETECTED MISSING STATE (no AI suggestion): row={row_idx}, city='{city_value}', state=null/empty")
                        if state_col:
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=state_col,
                                issue_type="MissingState",
                                dirty_value='',
                                suggested_value=correct_state if correct_state else f"[AI failed - please verify state for city '{city_value}']",
                                confidence=0.60 if correct_state else 0.40,
                                explanation=f"State is missing for city '{city_value}'" + (f". AI suggests: '{correct_state}'" if correct_state else " (AI could not determine state - quota exhausted)"),
                                why_agentic="🤖 AI-Generated (Gemini): Detects missing state data for geographic validation" + (" - AI quota exhausted, manual verification needed" if not correct_state else "")
                            ))
                    
                    # Now try to get AI suggestions and create issues
                    if correct_state:
                        # Now find country from state (more reliable than city alone)
                        cache_key_country = f"{correct_state}_country"
                        if cache_key_country in self.city_country_cache:
                            suggested_country = self.city_country_cache[cache_key_country]
                        else:
                            # Use AI to find country from state
                            print(f"DEBUG: Step 2 - Finding country for state '{correct_state}'")
                            suggested_country = self._find_country_from_state(correct_state, llm)
                            if suggested_country:
                                self.city_country_cache[cache_key_country] = suggested_country
                                print(f"DEBUG: ✅ AI returned country '{suggested_country}' for state '{correct_state}'")
                            else:
                                # Try to find country from city if state lookup failed (still using AI)
                                print(f"DEBUG: Finding country from city '{city_value}' (state lookup failed)")
                                suggested_country = self._find_country_from_city(city_value, llm)
                                if suggested_country:
                                    print(f"DEBUG: ✅ AI returned country '{suggested_country}' for city '{city_value}'")
                        
                        # Step 3: Validate and fix STATE
                        # ALWAYS check if state is wrong or missing - this is the PRIMARY detection
                        current_state_str = str(state_value).strip() if state_value else ''
                        correct_state_str = str(correct_state).strip() if correct_state else ''
                        
                        # Normalize both for comparison (remove extra spaces, case-insensitive, remove numeric prefixes)
                        if current_state_str:
                            current_state_str = re.sub(r'^\d+\s+', '', current_state_str)  # Remove numeric prefixes
                            state_normalized = re.sub(r'\s+', ' ', current_state_str.strip().lower())
                        else:
                            state_normalized = ''
                        
                        if correct_state_str:
                            correct_normalized = re.sub(r'\s+', ' ', correct_state_str.strip().lower())
                        else:
                            correct_normalized = ''
                        
                        # Check if state exists and is wrong
                        if state_normalized and state_normalized != correct_normalized:
                            # State is wrong - create issue (THIS IS THE PRIMARY DETECTION)
                            print(f"DEBUG: ⚠️ DETECTED INCORRECT STATE: row={row_idx}, city='{city_value}', current_state='{state_value}', correct_state='{correct_state}'")
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=state_col,
                                issue_type="IncorrectState",
                                dirty_value=state_value,
                                suggested_value=correct_state,
                                confidence=0.9,
                                explanation=f"State '{state_value}' is incorrect for city '{city_value}'. Correct state is '{correct_state}' (AI-determined)",
                                why_agentic="🤖 AI-Generated (Gemini): Uses AI geographic knowledge to determine the correct state for the city, then detects and fixes incorrect states"
                            ))
                        # If state is missing but we found one, create issue
                        elif not state_normalized or self._is_null_or_empty(state_value):
                            print(f"DEBUG: ⚠️ DETECTED MISSING STATE: row={row_idx}, city='{city_value}', correct_state='{correct_state}'")
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=state_col,
                                issue_type="MissingState",
                                dirty_value=state_value or '',
                                suggested_value=correct_state,
                                confidence=0.85,
                                explanation=f"State is missing for city '{city_value}'. Inferred state is '{correct_state}' (AI-determined)",
                                why_agentic="🤖 AI-Generated (Gemini): Uses AI geographic knowledge to infer missing state information from city names"
                            ))
                        
                        # Step 4: Validate and fix COUNTRY (using state we found)
                        if suggested_country:
                            country_col_to_use = country_col if country_col else (country_columns[0] if country_columns else 'country')
                            
                            if self._is_null_or_empty(country_value):
                                # Country is missing - use the one we found from state
                                print(f"DEBUG: Creating MissingCountry issue: row={row_idx}, city={city_value}, state={correct_state}, country='{suggested_country}'")
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=country_col_to_use,
                                    issue_type="MissingCountry",
                                    dirty_value=country_value or '',
                                    suggested_value=suggested_country,
                                    confidence=0.85,
                                    explanation=f"Country inferred from state '{correct_state}' (determined from city '{city_value}') (AI-determined)",
                                    why_agentic="🤖 AI-Generated (Gemini): First determines state from city using AI, then country from state for maximum accuracy"
                                ))
                            elif country_value and country_value.strip().lower() != suggested_country.strip().lower():
                                # Country exists but is wrong - suggest correction
                                print(f"DEBUG: Creating IncorrectCountry issue: row={row_idx}, city={city_value}, state={correct_state}, current_country='{country_value}', correct_country='{suggested_country}'")
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=country_col_to_use,
                                    issue_type="IncorrectCountry",
                                    dirty_value=country_value,
                                    suggested_value=suggested_country,
                                    confidence=0.85,
                                    explanation=f"Country '{country_value}' is incorrect for state '{correct_state}'. Correct country is '{suggested_country}' (AI-determined)",
                                    why_agentic="🤖 AI-Generated (Gemini): Validates country consistency with the state determined from city using AI"
                                ))
                    else:
                        # LLM failed to find state, but we still need to create issues for missing state/country
                        # This is the missing piece - when correct_state is None, we still need to flag issues!
                        
                        # Check if state is missing
                        if self._is_null_or_empty(state_value) and state_col:
                            print(f"DEBUG: Creating MissingState issue (AI failed): row={row_idx}, city='{city_value}'")
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=state_col,
                                issue_type="MissingState",
                                dirty_value='',
                                suggested_value=f"[AI failed - please verify state for city '{city_value}']",
                                confidence=0.40,
                                explanation=f"State is missing for city '{city_value}' (AI could not determine state - quota exhausted)",
                                why_agentic="🤖 AI-Generated (Gemini): Detects missing state data - AI quota exhausted, manual verification needed"
                            ))
                        
                        # Check if country is missing
                        if self._is_null_or_empty(country_value):
                            print(f"DEBUG: Creating MissingCountry issue (AI failed): row={row_idx}, city='{city_value}'")
                            country_col_to_use = country_col if country_col else (country_columns[0] if country_columns else 'country')
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=country_col_to_use,
                                issue_type="MissingCountry",
                                dirty_value='',
                                suggested_value=f"[AI failed - please verify country for city '{city_value}']",
                                confidence=0.40,
                                explanation=f"Country is missing for city '{city_value}' (AI could not determine country - quota exhausted)",
                                why_agentic="🤖 AI-Generated (Gemini): Detects missing country data - AI quota exhausted, manual verification needed"
                            ))
                
                # If no state column exists, still check if country is missing
                elif self._is_null_or_empty(country_value):
                    # Try to get country from AI
                    cache_key = f"{city_value}_country"
                    suggested_country = None
                    
                    if cache_key in self.city_country_cache:
                        suggested_country = self.city_country_cache[cache_key]
                    else:
                        # Use AI to find country from city
                        if llm:
                            print(f"DEBUG: Calling AI to find country for city '{city_value}' (no state column)")
                            suggested_country = self._find_country_from_city(city_value, llm)
                            if suggested_country:
                                print(f"DEBUG: ✅ AI returned country '{suggested_country}' for city '{city_value}'")
                                self.city_country_cache[cache_key] = suggested_country
                            else:
                                print(f"DEBUG: ⚠️ AI returned None for country of city '{city_value}'")
                    
                    # ALWAYS create issue for missing country, even if AI failed
                    target_col = country_col if country_col else (country_columns[0] if country_columns else 'country')
                    
                    issues.append(self._create_issue(
                        row_id=row_idx,
                        column=target_col,
                        issue_type="MissingCountry",
                        dirty_value=country_value or '',
                        suggested_value=suggested_country if suggested_country else f"[AI failed - please verify country for city '{city_value}']",
                        confidence=0.75 if suggested_country else 0.35,
                        explanation=f"Country is missing for city '{city_value}'" + (f". AI suggests: '{suggested_country}'" if suggested_country else " (AI could not determine country - quota exhausted)"),
                        why_agentic="🤖 AI-Generated (Gemini): Detects missing country data for geographic validation" + (" - AI quota exhausted, manual verification needed" if not suggested_country else "")
                    ))
                
        
        return issues
    
    def _find_country_from_state(self, state_name: str, llm) -> Optional[str]:
        """Find country from state/province name using AI only (no hardcoded fallbacks)"""
        if not llm:
            print(f"DEBUG: _find_country_from_state: No LLM client provided for state '{state_name}'")
            return None
        
        try:
            print(f"DEBUG: _find_country_from_state: Calling LLM for state '{state_name}', llm type: {type(llm)}")
            prompt = f"""What country is the state/province "{state_name}" located in?

Use your geographic knowledge to determine the country. Consider:
- Indian states: Maharashtra, Karnataka, Tamil Nadu, West Bengal, Gujarat, etc. → "India"
- US states: California, New York, Texas, Florida, etc. → "United States" or "USA"
- Canadian provinces: Ontario, Quebec, British Columbia, etc. → "Canada"
- Return the full country name (e.g., "India", "United States", "Canada")

Return ONLY a JSON object:
{{
    "country": "Country Name",
    "confidence": 0.0-1.0
}}

If you're not sure, return:
{{
    "country": null,
    "confidence": 0.0
}}"""
            
            from agents.llm_helper import call_llm
            print(f"DEBUG: _find_country_from_state: About to call call_llm for state '{state_name}'")
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a geographic knowledge assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=500
            )
            
            print(f"DEBUG: _find_country_from_state: LLM response for '{state_name}': {content[:200] if content else 'None'}")
            if not content:
                print(f"DEBUG: _find_country_from_state: No content returned from LLM for state '{state_name}'")
                return None
            
            # If content is just '{' or incomplete, try to get more
            if content.strip() == '{' or len(content.strip()) < 10:
                print(f"DEBUG: _find_country_from_state: Incomplete JSON response for '{state_name}', content: {content}")
                return None
            
            import json
            import re
            try:
                # Try to extract JSON from response
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object in the response
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                return result.get('country')
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON from LLM response for state {state_name}: {e}")
                print(f"Response content: {content[:200]}")
                return None
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower():
                print(f"❌ LLM API quota exhausted when finding country for state '{state_name}'. Cannot use AI.")
            else:
                print(f"❌ Error finding country for state {state_name}: {e}")
            return None
    
    def _find_country_from_city(self, city_name: str, llm) -> Optional[str]:
        """Find country from city name using AI only (no hardcoded fallbacks)"""
        if not llm:
            print(f"DEBUG: _find_country_from_city: No LLM client provided for city '{city_name}'")
            return None
        
        try:
            print(f"DEBUG: _find_country_from_city: Calling LLM for city '{city_name}', llm type: {type(llm)}")
            prompt = f"""What country is the city "{city_name}" located in?

Use your geographic knowledge to determine the country. Consider:
- Common city names (e.g., "Mumbai" is in India, "London" is in UK)
- If there are multiple cities with the same name, choose the most well-known one
- For Indian cities (Mumbai, Delhi, Bangalore, Chennai, Kolkata, Hyderabad, Pune, etc.), return "India"

Return ONLY a JSON object:
{{
    "country": "Country Name",
    "confidence": 0.0-1.0
}}

If you're not sure or the city name is ambiguous, return:
{{
    "country": null,
    "confidence": 0.0
}}"""
            
            from agents.llm_helper import call_llm
            print(f"DEBUG: _find_country_from_city: About to call call_llm for city '{city_name}'")
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a geographic knowledge assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=500
            )
            
            print(f"DEBUG: _find_country_from_city: LLM response for '{city_name}': {content[:200] if content else 'None'}")
            if not content:
                print(f"DEBUG: _find_country_from_city: No content returned from LLM for city '{city_name}'")
                return None
            
            # If content is just '{' or incomplete, try to get more
            if content.strip() == '{' or len(content.strip()) < 10:
                print(f"DEBUG: _find_country_from_city: Incomplete JSON response for '{city_name}', content: {content}")
                return None
            
            import json
            import re
            try:
                # Try to extract JSON from response (sometimes LLM adds extra text or code blocks)
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object in the response
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                return result.get('country')
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON from LLM response for city {city_name}: {e}")
                print(f"Response content: {content[:200]}")
                return None
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower():
                print(f"❌ LLM API quota exhausted when finding country for city '{city_name}'. Cannot use AI.")
            else:
                print(f"❌ Error finding country for city {city_name}: {e}")
            return None
    
    def _find_state_from_city(self, city_name: str, country_name: Optional[str] = None, llm=None) -> Optional[str]:
        """Find state/province from city name using AI only (no hardcoded fallbacks)"""
        if not llm:
            print(f"DEBUG: _find_state_from_city: No LLM client provided for city '{city_name}'")
            return None
        
        try:
            print(f"DEBUG: _find_state_from_city: Calling LLM for city '{city_name}', country: {country_name or 'None'}, llm type: {type(llm)}")
            country_context = f" in {country_name}" if country_name else ""
            prompt = f"""What state/province is the city "{city_name}"{country_context} located in?

CRITICAL: You must determine the CORRECT state/province for this city. Be precise and accurate.

For Indian cities:
- Mumbai, Pune, Nagpur, Nashik → Maharashtra
- Delhi, New Delhi → Delhi (or National Capital Territory of Delhi)
- Bangalore, Mysore, Mangalore → Karnataka
- Chennai, Coimbatore, Madurai → Tamil Nadu
- Kolkata, Darjeeling → West Bengal
- Hyderabad, Warangal → Telangana
- Ahmedabad, Surat, Vadodara → Gujarat
- Jaipur, Jodhpur, Udaipur → Rajasthan
- Lucknow, Kanpur, Agra → Uttar Pradesh

For US cities:
- New York City, Buffalo → New York
- Los Angeles, San Francisco, San Diego → California
- Chicago, Springfield → Illinois
- Houston, Dallas, Austin → Texas
- Miami, Tampa, Orlando → Florida

Return the FULL, OFFICIAL state/province name (e.g., "Maharashtra" not "MH", "California" not "CA", "Karnataka" not "KA").

Return ONLY a JSON object:
{{
    "state": "State/Province Name",
    "confidence": 0.0-1.0
}}

If you're not sure or the city name is ambiguous, return:
{{
    "state": null,
    "confidence": 0.0
}}"""
            
            from agents.llm_helper import call_llm
            print(f"DEBUG: _find_state_from_city: About to call call_llm for city '{city_name}'")
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a geographic knowledge assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=500
            )
            
            print(f"DEBUG: _find_state_from_city: LLM response for '{city_name}': {content[:200] if content else 'None'}")
            if not content:
                print(f"DEBUG: _find_state_from_city: No content returned from LLM for city '{city_name}'")
                return None
            
            # If content is just '{' or incomplete, try to get more
            if content.strip() == '{' or len(content.strip()) < 10:
                print(f"DEBUG: _find_state_from_city: Incomplete JSON response for '{city_name}', content: {content}")
                return None
            
            import json
            import re
            try:
                # Try to extract JSON from response (sometimes LLM adds extra text or code blocks)
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object in the response
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                return result.get('state')
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON from LLM response for state of city {city_name}: {e}")
                print(f"Response content: {content[:200]}")
                return None
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower():
                print(f"❌ Gemini API quota exhausted when finding state for city '{city_name}'. Cannot use AI.")
            else:
                print(f"❌ Error finding state for city {city_name}: {e}")
            return None
