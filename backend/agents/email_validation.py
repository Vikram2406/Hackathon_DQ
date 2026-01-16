"""
Email Validation Agent - Detects invalid email addresses
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
import re
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue


class EmailValidationAgent(BaseAgent):
    """Agent for detecting and fixing invalid email addresses"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Detect invalid email addresses
        
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
        
        # Data-driven: Analyze columns to find email columns (not just by name)
        from agents.data_analyzer import DataAnalyzer
        column_analysis = DataAnalyzer.analyze_column_types(dataset_rows)
        
        # Find email columns: either by name OR by data pattern (>50% emails)
        email_columns = []
        for col, analysis in column_analysis.items():
            col_lower = col.lower()
            # Check by name OR by detected type
            if (any(kw in col_lower for kw in ['email', 'e-mail', 'mail']) or 
                analysis.get('type') == 'email'):
                email_columns.append(col)
        
        # Learn most common email domain from data
        most_common_domain = None
        if email_columns:
            # Analyze first email column to find common domain
            domains = DataAnalyzer.detect_email_domains(dataset_rows, email_columns[0])
            if domains:
                most_common_domain = list(domains.keys())[0]  # Most common domain
        
        # Basic email regex pattern
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        # Common invalid patterns
        invalid_patterns = [
            r'@.*@',  # Multiple @ symbols
            r'\.\.',  # Consecutive dots
            r'^\.',   # Starts with dot
            r'\.$',   # Ends with dot
            r'@\.',   # @ followed by dot
            r'\.@',   # Dot before @
            r'^\s|\s$',  # Leading/trailing whitespace
        ]
        
        # Process each row
        for row_idx, row in enumerate(dataset_rows):
            for col in email_columns:
                value = row.get(col)
                if value and isinstance(value, str) and value.strip():
                    email = value.strip()
                    
                    # Check basic format
                    is_valid_format = email_pattern.match(email)
                    
                    # Check for invalid patterns
                    has_invalid_pattern = False
                    for pattern in invalid_patterns:
                        if re.search(pattern, email):
                            has_invalid_pattern = True
                            break
                    
                    # Check for common issues
                    issues_found = []
                    missing_at = '@' not in email
                    missing_domain = False
                    if '@' in email:
                        domain_part = email.split('@')[-1]
                        missing_domain = '.' not in domain_part
                    else:
                        missing_domain = True
                    
                    if not is_valid_format:
                        issues_found.append("Invalid email format")
                    if has_invalid_pattern:
                        issues_found.append("Contains invalid characters/patterns")
                    if missing_at:
                        issues_found.append("Missing @ symbol")
                    if missing_domain:
                        issues_found.append("Missing domain extension")
                    
                    # Try to suggest a fix using LLM (intelligent AI decision with data context)
                    suggested_value = None
                    if (not is_valid_format or has_invalid_pattern or missing_at or missing_domain):
                        # Always use LLM for intelligent fixes with data context
                        if llm:
                            # Get data context about this column
                            data_context = DataAnalyzer.get_data_context(dataset_rows, col)
                            suggested_value = self._llm_fix_email(email, llm, data_context, most_common_domain)
                        else:
                            # Basic cleanup attempt (fallback)
                            cleaned = email.replace(' ', '').replace('..', '.').strip()
                            if missing_at and not '@' in cleaned:
                                # Use most common domain from data, or default to gmail.com
                                domain_to_use = f"@{most_common_domain}" if most_common_domain else "@gmail.com"
                                cleaned = f"{cleaned}{domain_to_use}"
                            if email_pattern.match(cleaned):
                                suggested_value = cleaned
                    
                    # If no suggested value from LLM but missing @ or domain, add @gmail.com
                    if issues_found and not suggested_value:
                        if missing_at or missing_domain:
                            # Default to @gmail.com if LLM didn't suggest anything
                            if missing_at:
                                suggested_value = f"{email}@gmail.com"
                            elif missing_domain:
                                suggested_value = f"{email.split('@')[0]}@gmail.com"
                    
                    # Only create issue if we found problems AND have a different suggested value
                    if issues_found and suggested_value and suggested_value != email:
                        issues.append(self._create_issue(
                            row_id=row_idx,
                            column=col,
                            issue_type="InvalidEmail",
                            dirty_value=email,
                            suggested_value=suggested_value,
                            confidence=0.85,
                            explanation=f"Invalid email detected: {', '.join(issues_found)}. AI suggested correction: {suggested_value}",
                            why_agentic="🤖 AI-Powered: Uses Google Gemini AI to intelligently fix email addresses, adding missing @ symbols and domains (e.g., @gmail.com)"
                        ))
                    elif issues_found and not suggested_value:
                        # Issue found but can't fix - still report it
                        issues.append(self._create_issue(
                            row_id=row_idx,
                            column=col,
                            issue_type="InvalidEmail",
                            dirty_value=email,
                            suggested_value=None,  # Mark as unfixable
                            confidence=0.7,
                            explanation=f"Invalid email detected: {', '.join(issues_found)}. Cannot auto-fix.",
                            why_agentic="🤖 AI-Powered: AI detected invalid email but cannot suggest fix"
                        ))
        
        return issues
    
    def _llm_fix_email(self, email: str, llm, data_context: str = "", most_common_domain: Optional[str] = None) -> Optional[str]:
        """Use LLM to intelligently suggest email correction based on actual data patterns"""
        try:
            domain_hint = f"\n\nData context: The most common email domain in this dataset is '{most_common_domain}'. Use this domain when suggesting fixes." if most_common_domain else ""
            context_hint = f"\n\nColumn data context: {data_context}" if data_context else ""
            
            prompt = f"""Fix this invalid email address intelligently: "{email}"{domain_hint}{context_hint}

CRITICAL RULES - FOLLOW THESE EXACTLY:
1. If the email is missing @ symbol: ALWAYS add @gmail.com (NOT @apple.com, NOT @company.com, ONLY @gmail.com)
2. If the email is missing domain: ALWAYS add @gmail.com (NOT @apple.com, NOT @company.com, ONLY @gmail.com)
3. If the email is missing domain extension: ALWAYS use @gmail.com (NOT @apple.com, NOT @company.com, ONLY @gmail.com)
4. NEVER suggest @apple.com, @company.com, or any company-specific domain unless the email already contains that domain
5. The DEFAULT domain for ALL fixes is @gmail.com - use this unless the email already has a valid domain

Common issues to fix:
- Missing @ symbol: add @gmail.com (DEFAULT - use this always)
- Missing domain: add @gmail.com (DEFAULT - use this always)
- Missing domain extension: add @gmail.com (DEFAULT - use this always)
- Typos: correct common mistakes (gmail.com, yahoo.com, etc.)
- Extra spaces: remove them
- Double dots: fix to single dot

Examples (IMPORTANT - follow these patterns):
- "john.doe" → "john.doe@gmail.com" (NOT @apple.com)
- "Paul" → "Paul@gmail.com" (NOT @apple.com, NOT @company.com)
- "user@domain" → "user@gmail.com" (if missing extension, use @gmail.com)
- "user @gmail.com" → "user@gmail.com" (remove space)

Return ONLY a JSON object:
{{
    "fixed": "corrected@gmail.com",
    "confidence": 0.0-1.0,
    "explanation": "brief explanation"
}}

If the email cannot be fixed, return:
{{
    "fixed": null,
    "confidence": 0.0,
    "explanation": "Cannot be fixed"
}}"""
            
            # Use unified LLM helper
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are an intelligent email validation assistant. Analyze the actual data patterns provided and make decisions based on what's common in the dataset, not hardcoded rules. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=150
            )
            
            if not content:
                return None
            
            try:
                # Try to extract JSON from response (sometimes LLM adds extra text or code blocks)
                import re
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*', '', content)
                content = content.strip()
                
                # Look for JSON object
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                result = json.loads(content)
                fixed = result.get('fixed')
                
                # CRITICAL: If LLM suggests @apple.com or any non-standard domain for missing @, override to @gmail.com
                if fixed and '@' not in email:
                    # Original email had no @, so LLM should have added @gmail.com
                    if '@apple.com' in fixed.lower() or '@company.com' in fixed.lower() or '@microsoft.com' in fixed.lower():
                        # Override: use @gmail.com instead
                        fixed = f"{email}@gmail.com"
                        print(f"DEBUG: EmailValidationAgent - Overrode LLM suggestion to use @gmail.com instead of company domain")
                
                # If LLM didn't suggest anything but email is missing @, add @gmail.com
                if not fixed and '@' not in email:
                    return f"{email}@gmail.com"
                
                # Final check: if email was missing @ and fixed doesn't have @gmail.com, ensure it does
                if fixed and '@' not in email and '@gmail.com' not in fixed.lower():
                    # Extract the local part (before any @ that might have been added incorrectly)
                    local_part = email.strip()
                    fixed = f"{local_part}@gmail.com"
                    print(f"DEBUG: EmailValidationAgent - Ensured @gmail.com is used for email missing @ symbol")
                
                return fixed
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON from LLM response: {e}")
                print(f"Response content: {content[:200]}")
                # Fallback: if missing @, add @gmail.com
                if '@' not in email:
                    return f"{email}@gmail.com"
                return None
        except Exception as e:
            print(f"Error in LLM email fix: {e}")
            return None
