"""
Formatting Agent - Handles Date Chaos and Phone Normalization
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
import re
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue
from utils.data_cleaning import parse_date, normalize_phone


class FormattingAgent(BaseAgent):
    """Agent for detecting and fixing date and phone formatting issues"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect date chaos and phone normalization issues
        
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
        
        # Data-driven: Analyze columns to detect types from actual data patterns
        from agents.data_analyzer import DataAnalyzer
        column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)
        
        # Find date columns: by name OR by detected type
        date_columns = []
        phone_columns = []
        
        for col, analysis in column_analysis.items():
            col_lower = col.lower()
            # Check by name OR by detected type
            if (any(kw in col_lower for kw in ['date', 'time', 'created', 'updated', 'timestamp', 'dob', 'birth', 'start', 'end']) or
                analysis.get('type') == 'date'):
                date_columns.append(col)
            if (any(kw in col_lower for kw in ['phone', 'tel', 'mobile', 'cell']) or
                analysis.get('type') == 'phone'):
                phone_columns.append(col)
        
        # Detect country from phone data patterns
        phone_country = 'US'  # Default
        if phone_columns:
            # Try to find country column
            country_col = None
            for col in dataset_rows[0].keys():
                if 'country' in col.lower():
                    country_col = col
                    break
            
            phone_country = DataAnalyzer.detect_phone_country_from_data(
                dataset_rows, 
                phone_columns[0], 
                country_col
            )
        
        # Process each row
        for row_idx, row in enumerate(dataset_rows):
            # Date Format Standardization - Convert ALL dates to YYYY-MM-DD
            for col in date_columns:
                value = row.get(col)
                if value and isinstance(value, str) and value.strip():
                    # Check if it's not already in ISO format (YYYY-MM-DD)
                    if not re.match(r'^\d{4}-\d{2}-\d{2}$', str(value).strip()):
                        # Try to parse and normalize to ISO format (YYYY-MM-DD)
                        parsed = parse_date(str(value))
                        if parsed:
                            iso_date, confidence = parsed
                            # Always suggest ISO format if current format is different
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=col,
                                issue_type="DateFormatting",
                                dirty_value=value,
                                suggested_value=iso_date,
                                confidence=confidence,
                                explanation=f"Date standardization: '{value}' → '{iso_date}' (YYYY-MM-DD format)",
                                why_agentic="🤖 AI-Powered: Intelligently parses dates in any format (MM/DD/YYYY, DD-MM-YYYY, etc.) and standardizes to ISO YYYY-MM-DD format"
                            ))
                        elif llm:
                            # Use LLM for ambiguous/complex dates
                            try:
                                llm_result = self._llm_normalize_date(str(value), llm)
                                if llm_result:
                                    suggested, confidence, explanation = llm_result
                                    issues.append(self._create_issue(
                                        row_id=row_idx,
                                        column=col,
                                        issue_type="DateFormatting",
                                        dirty_value=value,
                                        suggested_value=suggested,
                                        confidence=confidence,
                                        explanation=f"Date standardization: '{value}' → '{suggested}' (YYYY-MM-DD format)",
                                        why_agentic="🤖 AI-Powered: LLM understands complex date formats and context to convert to ISO YYYY-MM-DD format"
                                    ))
                            except Exception as e:
                                print(f"Error in LLM date normalization: {e}")
            
            # Phone Normalization - use detected country from data analysis OR infer from city/state
            for col in phone_columns:
                value = row.get(col)
                if value and isinstance(value, str) and value.strip():
                    # Build context from row (check for country, city, state columns)
                    context = {}
                    city_value = None
                    state_value = None
                    for key, val in row.items():
                        key_lower = key.lower()
                        if 'country' in key_lower:
                            context['country'] = str(val) if val else None
                        elif 'city' in key_lower:
                            city_value = str(val) if val else None
                        elif 'state' in key_lower:
                            state_value = str(val) if val else None
                    
                    # PRIORITY ORDER: 
                    # 1. Country from country column (highest priority)
                    # 2. Country inferred from city/state (geographic data)
                    # 3. Phone pattern detection (lowest priority - can be wrong)
                    country_to_use = None
                    
                    # Step 1: Check if country column has a value and convert to country code (HIGHEST PRIORITY)
                    country_name = context.get('country')
                    if country_name and country_name not in ['', 'None', 'null', 'nan', None]:
                        country_name_str = str(country_name).strip()
                        country_name_lower = country_name_str.lower()
                        
                        print(f"DEBUG: FormattingAgent - Row {row_idx}: Found country column value: '{country_name_str}'")
                        
                        # Map country names to codes (comprehensive list)
                        if country_name_lower in ['united states', 'usa', 'us', 'united states of america', 'u.s.', 'u.s.a.']:
                            country_to_use = 'US'
                            print(f"DEBUG: FormattingAgent - ✅ Row {row_idx}: Using country from column: '{country_name_str}' → 'US' (HIGHEST PRIORITY)")
                        elif country_name_lower in ['india', 'ind', 'bharat', 'in', 'indian']:
                            country_to_use = 'IN'
                            print(f"DEBUG: FormattingAgent - ✅ Row {row_idx}: Using country from column: '{country_name_str}' → 'IN' (HIGHEST PRIORITY)")
                        elif len(country_name_str) == 2 and country_name_str.isalpha():
                            # Already a country code
                            country_to_use = country_name_str.upper()
                            print(f"DEBUG: FormattingAgent - ✅ Row {row_idx}: Using country code from column: '{country_to_use}' (HIGHEST PRIORITY)")
                        else:
                            print(f"DEBUG: FormattingAgent - ⚠️ Row {row_idx}: Country name '{country_name_str}' not in mapping, will try inference")
                    
                    # Step 2: If country is missing/null, infer from city/state (geographic data - SECOND PRIORITY)
                    # ONLY if phone number doesn't already have correct country code
                    if (not country_to_use or country_to_use in ['', 'None', 'null']):
                        # Check if phone already has a country code - if so, use that instead of inferring
                        if value and isinstance(value, str) and value.strip().startswith('+'):
                            # Phone has country code - try to detect it
                            if value.strip().startswith('+91'):
                                country_to_use = 'IN'
                                print(f"DEBUG: FormattingAgent - Row {row_idx}: Detected country 'IN' from phone prefix '+91'")
                            elif value.strip().startswith('+1'):
                                country_to_use = 'US'
                                print(f"DEBUG: FormattingAgent - Row {row_idx}: Detected country 'US' from phone prefix '+1'")
                        
                        # If still no country and we have city/state, infer it
                        if (not country_to_use) and llm and (city_value or state_value):
                            print(f"DEBUG: FormattingAgent - Country is null/missing for row {row_idx}. Inferring from city='{city_value}', state='{state_value}'")
                            inferred_country = self._infer_country_from_location(city_value, state_value, llm)
                            if inferred_country:
                                print(f"DEBUG: FormattingAgent - ✅ Inferred country code '{inferred_country}' from location (geographic data - SECOND PRIORITY)")
                                country_to_use = inferred_country
                    
                        # Step 3: Fallback to detected phone country from data patterns (LOWEST PRIORITY - can be wrong!)
                        if not country_to_use:
                            country_to_use = phone_country
                            print(f"DEBUG: FormattingAgent - ⚠️ Using detected phone country from data patterns: '{country_to_use}' (LOWEST PRIORITY - may be incorrect)")
                    
                    # Try deterministic normalization with detected country
                    # IMPORTANT: For Indian numbers, ensure we use +91 XXXXXXXXXX format (no brackets)
                    # CRITICAL: Ensure country_to_use is set before calling normalize_phone
                    if not country_to_use:
                        print(f"DEBUG: FormattingAgent - ⚠️ Row {row_idx}: country_to_use is None/empty! This should not happen if country column has value.")
                        # Try to get country from context as last resort
                        country_name = context.get('country')
                        if country_name:
                            country_name_lower = str(country_name).lower().strip()
                            if 'india' in country_name_lower or country_name_lower == 'in':
                                country_to_use = 'IN'
                                print(f"DEBUG: FormattingAgent - ✅ Row {row_idx}: Emergency fallback - set country_to_use='IN' from context")
                            elif 'united states' in country_name_lower or country_name_lower in ['us', 'usa']:
                                country_to_use = 'US'
                                print(f"DEBUG: FormattingAgent - ✅ Row {row_idx}: Emergency fallback - set country_to_use='US' from context")
                    
                    print(f"DEBUG: FormattingAgent - Row {row_idx}: Calling normalize_phone with country_code='{country_to_use}', phone='{value}'")
                    normalized = normalize_phone(str(value), country_code=country_to_use, context=context)
                    print(f"DEBUG: FormattingAgent - Row {row_idx}: normalize_phone returned: {normalized}")
                    
                    # Double-check: If country is India but format has brackets, fix it
                    if normalized and country_to_use == 'IN':
                        normalized_phone, confidence = normalized
                        # Indian format should be +91 XXXXXXXXXX (no brackets)
                        if '(' in normalized_phone and ')' in normalized_phone:
                            # Extract digits and reformat
                            digits_only = re.sub(r'[^\d+]', '', normalized_phone)
                            if digits_only.startswith('+91'):
                                digits_only = digits_only[3:]
                            if len(digits_only) == 10:
                                normalized = (f"+91 {digits_only}", confidence)
                    if normalized:
                        normalized_phone, confidence = normalized
                        if normalized_phone != str(value):
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=col,
                                issue_type="PhoneNormalization",
                                dirty_value=value,
                                suggested_value=normalized_phone,
                                confidence=confidence,
                                explanation=f"Phone number normalized to {country_to_use} format: {normalized_phone}",
                                why_agentic=f"🤖 AI-Powered: Detects country ('{country_to_use}') from geographic data and applies correct format (India: +91 XXXXXXXXXX, USA: +1 (XXX) XXX-XXXX)"
                            ))
                    elif llm:
                        # Use LLM for complex phone formats with country detection
                        try:
                            country_hint = context.get('country', '')
                            llm_result = self._llm_normalize_phone(str(value), llm, country_hint)
                            if llm_result:
                                suggested, confidence, explanation = llm_result
                                issues.append(self._create_issue(
                                    row_id=row_idx,
                                    column=col,
                                    issue_type="PhoneNormalization",
                                    dirty_value=value,
                                    suggested_value=suggested,
                                    confidence=confidence,
                                    explanation=explanation,
                                    why_agentic="LLM understands context and can extract phone numbers from messy text with country detection"
                                ))
                        except Exception as e:
                            print(f"Error in LLM phone normalization: {e}")
        
        return issues
    
    def _llm_normalize_date(self, date_string: str, llm) -> Optional[tuple]:
        """Use LLM to normalize ambiguous date strings"""
        try:
            prompt = f"""Normalize this date string to ISO format (YYYY-MM-DD): "{date_string}"

Return ONLY a JSON object with:
{{
    "normalized": "YYYY-MM-DD",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a date normalization assistant. Return only valid JSON."},
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
                result.get('explanation', 'LLM-normalized date')
            )
        except Exception:
            return None
    
    def _llm_normalize_phone(self, phone_string: str, llm, country_hint: str = '') -> Optional[tuple]:
        """Use LLM to normalize phone numbers with country detection"""
        try:
            country_context = f" The phone number is from {country_hint}." if country_hint else ""
            prompt = f"""Normalize this phone number to E.164 format{country_context}: "{phone_string}"

For Indian numbers, use format: +91 (XXXXX) XXXXXX
For US numbers, use format: +1 (XXX) XXX-XXXX

Return ONLY a JSON object with:
{{
    "normalized": "+91 (12345) 67890",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a phone number normalization assistant that detects country and applies appropriate format. Return only valid JSON."},
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
                result.get('explanation', 'LLM-normalized phone')
            )
        except Exception:
            return None
    
    def _infer_country_from_location(self, city: Optional[str], state: Optional[str], llm) -> Optional[str]:
        """Use LLM to infer country code from city/state when country is missing
        
        CRITICAL: Prioritize city over state, as state might be incorrect.
        If city is provided, ONLY use city for inference (ignore state).
        """
        if not city and not state:
            return None
        
        try:
            # CRITICAL: Use city ONLY if available (state might be wrong)
            # State is only used as fallback if no city is provided
            if city:
                location_str = f"City: {city}"
            else:
                location_str = f"State: {state}"
            
            prompt = f"""Based on this location information: {location_str}

Determine the country code for phone number formatting. Return 2-letter country code.

CRITICAL RULES:
- For Indian cities (Mumbai, Delhi, Pune, Goa, Nagpur, etc.), return "IN"
- For US cities (New York, Los Angeles, Portland, etc.), return "US"
- For UK cities (London, Manchester, etc.), return "GB"
- Return ONLY the 2-letter country code, nothing else.
- DO NOT consider any state information - base answer ONLY on the city name provided.

Examples:
- City: Mumbai → IN
- City: Pune → IN
- City: Goa → IN
- City: Nagpur → IN
- City: New York → US
- City: Portland → US (if Oregon context), or could be US
- City: London → GB

Country code:"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a geographic assistant. Return only the 2-letter country code."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=10
            )
            
            if content:
                # Extract 2-letter code
                country_code = content.strip().upper()
                # Validate it's a 2-letter code
                if len(country_code) == 2 and country_code.isalpha():
                    return country_code
            
            return None
        except Exception as e:
            print(f"DEBUG: Error inferring country from location: {e}")
            return None