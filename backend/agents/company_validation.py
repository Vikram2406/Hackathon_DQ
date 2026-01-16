"""
Company Validation Agent - Validates company names using web search and LLM
"""
import sys
import os
import json
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue


class CompanyValidationAgent(BaseAgent):
    """Agent for validating and correcting company names using web search"""
    
    def __init__(self, llm_client=None):
        super().__init__(llm_client)
        self.company_cache = {}  # Cache validated companies
        # Override category name for better display
        self.category = "CompanyValidation"
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect and validate company names
        
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
        
        # Data-driven: Analyze columns to find company columns from data patterns
        from agents.data_analyzer import DataAnalyzer
        column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)
        
        # Find company columns: by name OR by analyzing data patterns (high uniqueness, text type)
        company_columns = []
        for col, analysis in column_analysis.items():
            col_lower = col.lower()
            
            # CRITICAL: Exclude non-company columns (measurements, locations, dates, contact info)
            excluded_keywords = ['height', 'weight', 'length', 'width', 'distance', 'size', 
                                'measurement', 'city', 'state', 'country', 'address', 'street',
                                'email', 'phone', 'date', 'time', 'birth', 'age', 'id', 'number']
            
            if any(kw in col_lower for kw in excluded_keywords):
                continue  # Skip this column - it's not a company column
            
            # Check by name only for company columns (be very strict)
            if any(kw in col_lower for kw in ['company', 'organisation', 'organization', 'org', 'corp', 'firm', 'employer', 'business']):
                company_columns.append(col)
                print(f"DEBUG: CompanyValidationAgent - Detected company column: '{col}'")
        
        if not company_columns:
            return issues
        
        # STEP 1: Find email column to infer company from corporate email domains
        email_column = None
        for col in dataset_rows[0].keys():
            col_lower = col.lower()
            if 'email' in col_lower or 'mail' in col_lower:
                email_column = col
                print(f"DEBUG: CompanyValidationAgent - Detected email column: '{email_column}'")
                break
        
        # STEP 2: Build a mapping of email domains to company names (for corporate emails)
        # Also track rows with generic emails (gmail, yahoo, etc.) to SKIP all validation
        email_to_company = {}  # {row_idx: inferred_company_from_email}
        rows_with_generic_email = set()  # Rows with gmail.com, yahoo.com, etc. - NO validation
        
        generic_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 
                          'mail.com', 'protonmail.com', 'aol.com', 'live.com', 'msn.com',
                          'ymail.com', 'gmx.com', 'zoho.com', 'fastmail.com']
        
        if email_column:
            for row_idx, row in enumerate(dataset_rows):
                email_value = row.get(email_column)
                if email_value and isinstance(email_value, str) and '@' in email_value:
                    # Extract domain from email
                    try:
                        domain = email_value.split('@')[1].strip().lower()
                        
                        if domain in generic_domains:
                            # Generic email - SKIP ALL validation for this row
                            rows_with_generic_email.add(row_idx)
                            print(f"DEBUG: CompanyValidationAgent - Row {row_idx}: Generic email domain '{domain}' - will SKIP company validation")
                        elif llm:
                            # Corporate email - infer company name
                            inferred_company = self._infer_company_from_domain(domain, llm)
                            if inferred_company:
                                email_to_company[row_idx] = inferred_company
                                print(f"DEBUG: CompanyValidationAgent - Row {row_idx}: Inferred company '{inferred_company}' from email domain '{domain}'")
                    except Exception as e:
                        pass  # Skip malformed emails
        
        # STEP 3: Collect all unique company names
        company_names = {}
        for row_idx, row in enumerate(dataset_rows):
            for col in company_columns:
                value = row.get(col)
                if value and isinstance(value, str) and value.strip():
                    company = value.strip()
                    if company not in company_names:
                        company_names[company] = []
                    company_names[company].append((row_idx, col))
        
        # Find the most common company name (likely the correct one)
        # BUT: Prefer FULL names over abbreviations (e.g., "Microsoft" over "MS")
        if len(company_names) > 1:
            # Get all company names and their frequencies
            company_freq = {name: len(locs) for name, locs in company_names.items()}
            
            # Use AI to determine the canonical name (prefer full names)
            canonical_name = None
            if llm:
                # Ask AI which is the canonical name (should prefer full names)
                all_names = list(company_names.keys())
                canonical_result = self._find_canonical_company_name(all_names, llm)
                if canonical_result:
                    canonical_name = canonical_result
                else:
                    # Fallback to most common
                    canonical_name = max(company_freq.items(), key=lambda x: x[1])[0]
            else:
                # Without AI, prefer longer names (likely full names) over short ones (abbreviations)
                # Sort by length first, then by frequency
                sorted_names = sorted(company_freq.items(), key=lambda x: (len(x[0]), x[1]), reverse=True)
                canonical_name = sorted_names[0][0] if sorted_names else max(company_freq.items(), key=lambda x: x[1])[0]
            
            most_common_company = canonical_name
        else:
            most_common_company = None
        
        # STEP 4: Validate each row's company (including email-based validation)
        rows_validated_by_email = set()
        
        # First pass: Validate companies based on email domains (SKIP generic emails)
        for row_idx, row in enumerate(dataset_rows):
            # CRITICAL: Skip rows with generic email domains (gmail.com, yahoo.com, etc.)
            if row_idx in rows_with_generic_email:
                continue  # Keep company as-is (even if null) for generic emails
            
            email_inferred_company = email_to_company.get(row_idx)
            if email_inferred_company:
                for col in company_columns:
                    company_value = row.get(col)
                    if company_value and isinstance(company_value, str) and company_value.strip():
                        company = company_value.strip()
                        # Compare with email-inferred company (case-insensitive, but also check for abbreviations)
                        if company.lower() != email_inferred_company.lower():
                            # Company name doesn't match email domain - flag as issue
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=col,
                                issue_type="CompanyMismatch",
                                dirty_value=company,
                                suggested_value=email_inferred_company,
                                confidence=0.95,
                                explanation=f"Company '{company}' doesn't match corporate email domain. Email suggests '{email_inferred_company}'",
                                why_agentic=f"🤖 AI-Powered: Infers company from corporate email domain (e.g., john@microsoft.com → Microsoft)"
                            ))
                            rows_validated_by_email.add(row_idx)
        
        # Second pass: Standard company validation (for rows not validated by email, excluding generic emails)
        for company, locations in company_names.items():
            # Filter out locations for rows with generic emails
            locations_to_validate = [(row_idx, col) for row_idx, col in locations 
                                     if row_idx not in rows_with_generic_email]
            
            if not locations_to_validate:
                continue  # All rows for this company have generic emails - skip
            
            # Skip if this is the canonical one (the one we want to standardize to)
            if company == most_common_company and len(company_names) > 1:
                continue
            
            # Check cache first
            if company in self.company_cache:
                validation_result = self.company_cache[company]
            else:
                # Use AI to validate company name (no hardcoded fallbacks)
                if llm:
                    validation_result = self._validate_company(company, llm, most_common_company)
                else:
                    validation_result = None
                
                if validation_result:
                    self.company_cache[company] = validation_result
            
            # Create issues if:
            # 1. Validation found issues (not valid)
            # 2. There's a corrected name different from current
            # 3. There are multiple company variations (standardize to most common)
            should_create_issue = False
            suggested_value = company  # Default to keeping original
            
            if validation_result:
                if not validation_result.get('is_valid', True):
                    should_create_issue = True
                    suggested_value = validation_result.get('corrected_name') or most_common_company or company
                elif validation_result.get('corrected_name') and validation_result.get('corrected_name') != company:
                    should_create_issue = True
                    suggested_value = validation_result.get('corrected_name')
            elif len(company_names) > 1 and most_common_company:
                # Multiple variations - suggest standardizing to most common
                should_create_issue = True
                suggested_value = most_common_company
            
            if should_create_issue and suggested_value != company:
                confidence = validation_result.get('confidence', 0.75) if validation_result else 0.7
                explanation = validation_result.get('explanation', f'Standardize to most common company name: {suggested_value}') if validation_result else f'Multiple company name variations detected. Standardizing to most common: {suggested_value}'
                
                # Use locations_to_validate (already filtered to exclude generic emails)
                for row_idx, col in locations_to_validate:
                    # Skip if already validated by email
                    if row_idx in rows_validated_by_email:
                        continue
                    
                    issues.append(self._create_issue(
                        row_id=row_idx,
                        column=col,
                        issue_type="CompanyValidation",
                        dirty_value=company,
                        suggested_value=suggested_value,
                        confidence=confidence,
                        explanation=explanation,
                        why_agentic="🤖 AI-Powered: Google Gemini AI analyzes all company names in dataset, identifies variations, typos, and abbreviations (e.g., 'MS' → 'Microsoft'), and suggests the most likely correct/canonical name"
                    ))
        
        return issues
    
    def _validate_company(self, company_name: str, llm, canonical_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Validate company name using LLM with intelligent reasoning"""
        if not llm:
            return None
        
        try:
            context = f"\n\nIMPORTANT: The canonical company name in this dataset is '{canonical_name}'. All variations should be standardized to this canonical name." if canonical_name else ""
            
            # Use LLM to validate and suggest corrections with intelligent reasoning
            prompt = f"""Analyze this company name intelligently: "{company_name}"{context}

Your task:
1. Check if this is a real, well-known company (especially Indian companies like TCS, Infosys, Wipro, HCL, Tech Mahindra, etc.)
2. Look for common typos, misspellings, abbreviations, or variations
3. Compare with the canonical company name in the dataset (if provided)
4. Suggest the correct/canonical FULL name if different

CRITICAL RULES:
- ALWAYS prefer FULL company names over abbreviations
- If canonical name is "Microsoft", then "MS", "MIC", "M Soft", "Micro Soft" should ALL become "Microsoft"
- If canonical name is "Tata Consultancy Services", then "TCS", "Tata CS" should become "Tata Consultancy Services"
- Expand abbreviations to full names: "MS" → "Microsoft", "IBM" → "International Business Machines" (or keep "IBM" if it's the canonical)
- Fix typos: "Microsft" → "Microsoft", "Microsot" → "Microsoft"
- For Indian companies:
  * "TCS" → "Tata Consultancy Services"
  * "Infy" or "Infosys Tech" → "Infosys"
  * "Wipro Tech" → "Wipro"
  * "HCL Tech" → "HCL Technologies"
  * "TechM" → "Tech Mahindra"

Examples (canonical is "Microsoft"):
- "Microsoft" → Keep as "Microsoft" (already canonical)
- "MS" → "Microsoft" (expand abbreviation)
- "MIC" → "Microsoft" (expand abbreviation)
- "M Soft" → "Microsoft" (expand variation)
- "Microsft" → "Microsoft" (fix typo)
- "Micro Soft" → "Microsoft" (fix spacing)

Examples (canonical is "Tata Consultancy Services"):
- "TCS" → "Tata Consultancy Services" (expand to canonical)
- "Tata Consultancy Services" → Keep as is (already canonical)
- "Tata CS" → "Tata Consultancy Services"

Examples (canonical is "Infosys"):
- "Infosys" → Keep as is
- "Infy" → "Infosys"
- "Infosys Technologies" → "Infosys" (if canonical is just "Infosys")

Be intelligent: If you see multiple variations of the same company, suggest the most common/canonical one. Always expand abbreviations to full names when a canonical name is provided.

Return ONLY a JSON object:
{{
    "is_valid": true/false,
    "corrected_name": "correct company name or null",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation of your reasoning"
}}

If the company name is correct and well-known, return:
{{
    "is_valid": true,
    "corrected_name": null,
    "confidence": 0.9,
    "explanation": "Company name appears valid"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are an expert company validation assistant with extensive knowledge of global companies, especially Indian companies like TCS, Infosys, Wipro, HCL, etc. You can identify typos, suggest canonical names, and make intelligent decisions based on company recognition. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=200
            )
            
            if not content:
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
                return result
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON from LLM response: {e}")
                print(f"Response content: {content[:200]}")
                return None
        except Exception as e:
            print(f"Error validating company {company_name}: {e}")
            return None
    
    def _find_canonical_company_name(self, company_names: List[str], llm) -> Optional[str]:
        """Use AI to find the canonical company name (prefer full names over abbreviations)"""
        if not llm or not company_names:
            return None
        
        try:
            names_str = ', '.join([f'"{name}"' for name in company_names])
            prompt = f"""Given these company name variations: [{names_str}]

Determine which is the CANONICAL (official, full) company name. 

IMPORTANT RULES:
1. Prefer FULL names over abbreviations (e.g., "Microsoft" over "MS")
2. Prefer official company names over variations
3. If you see "Microsoft", "MS", "MIC", "M Soft" → canonical is "Microsoft"
4. If you see "TCS" and "Tata Consultancy Services" → canonical is "Tata Consultancy Services"

Return ONLY a JSON object:
{{
    "canonical_name": "Microsoft",
    "explanation": "brief explanation"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are an expert at identifying canonical company names. Always prefer full official names over abbreviations. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            if not content:
                return None
            
            import json
            import re
            try:
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                canonical = result.get('canonical_name')
                # Verify it's one of the input names
                if canonical in company_names:
                    return canonical
            except Exception as e:
                print(f"Error parsing canonical name: {e}")
            
            return None
        except Exception as e:
            print(f"Error finding canonical company name: {e}")
            return None
    
    def _infer_company_from_domain(self, domain: str, llm) -> Optional[str]:
        """
        Infer company name from email domain using AI
        
        Examples:
            microsoft.com → Microsoft
            apple.com → Apple
            netflix.com → Netflix
            ibm.com → IBM
        """
        if not llm:
            return None
        
        try:
            prompt = f"""What is the full official company name for the email domain: {domain}?

Examples:
- microsoft.com → Microsoft
- apple.com → Apple
- netflix.com → Netflix
- ibm.com → IBM
- google.com → Google

CRITICAL RULES:
1. Return the FULL, OFFICIAL company name (not abbreviation)
2. Use proper capitalization
3. Return ONLY a JSON object with the company name
4. If the domain is not a known company (e.g., personal domain), return null

Return ONLY this JSON format:
{{
    "company_name": "Full Company Name" or null
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are an expert at inferring company names from email domains. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            if not content:
                return None
            
            import json
            import re
            try:
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                company_name = result.get('company_name')
                if company_name and company_name.lower() != 'null':
                    print(f"DEBUG: _infer_company_from_domain - domain '{domain}' → company '{company_name}'")
                    return company_name
            except Exception as e:
                print(f"Error parsing company name from domain: {e}")
            
            return None
        except Exception as e:
            print(f"Error inferring company from domain: {e}")
            return None
