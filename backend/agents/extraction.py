"""
Extraction Agent - Handles Metadata Scraping (extracting structured data from text)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
import re
from agents.base_agent import BaseAgent
from models.schemas import AgenticIssue


class ExtractionAgent(BaseAgent):
    """Agent for extracting structured metadata from unstructured text fields"""
    
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Extract structured data (emails, URLs, names) from text fields
        
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
        
        # Find text columns that might contain extractable data
        text_columns = []
        for col in dataset_rows[0].keys():
            col_lower = col.lower()
            # Skip columns that are already structured (email, phone, etc.)
            if not any(kw in col_lower for kw in ['email', 'phone', 'url', 'name', 'id']):
                # Check if column contains long text
                sample_values = [str(row.get(col, '')) for row in dataset_rows[:10] if row.get(col)]
                if any(len(v) > 20 for v in sample_values):
                    text_columns.append(col)
        
        # Process each row
        for row_idx, row in enumerate(dataset_rows):
            for col in text_columns:
                value = row.get(col)
                if value and isinstance(value, str) and len(value) > 10:
                    # Try regex extraction first
                    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', value)
                    url_match = re.search(r'https?://[^\s]+', value)
                    
                    extracted = {}
                    if email_match and 'email' not in [k.lower() for k in row.keys()]:
                        extracted['email'] = email_match.group(0)
                    if url_match and 'url' not in [k.lower() for k in row.keys()]:
                        extracted['url'] = url_match.group(0)
                    
                    if extracted:
                        # Suggest creating new columns or filling existing ones
                        for field, extracted_value in extracted.items():
                            issues.append(self._create_issue(
                                row_id=row_idx,
                                column=col,
                                issue_type="MetadataScraping",
                                dirty_value=value[:50] + '...' if len(value) > 50 else value,
                                suggested_value=f"Extract {field}: {extracted_value}",
                                confidence=0.9,
                                explanation=f"Found {field} in text field: {extracted_value}",
                                why_agentic="Pulls structured data out of strings like emails or URLs"
                            ))
                    elif llm:
                        # Use LLM for complex extraction
                        try:
                            llm_result = self._llm_extract_metadata(value, llm)
                            if llm_result:
                                for field, extracted_value in llm_result.items():
                                    issues.append(self._create_issue(
                                        row_id=row_idx,
                                        column=col,
                                        issue_type="MetadataScraping",
                                        dirty_value=value[:50] + '...' if len(value) > 50 else value,
                                        suggested_value=f"Extract {field}: {extracted_value}",
                                        confidence=0.7,
                                        explanation=f"LLM extracted {field}: {extracted_value}",
                                        why_agentic="LLM can extract structured data from complex unstructured text"
                                    ))
                        except Exception as e:
                            print(f"Error in LLM metadata extraction: {e}")
        
        return issues
    
    def _llm_extract_metadata(self, text: str, llm) -> Dict[str, str]:
        """Use LLM to extract metadata from text"""
        try:
            prompt = f"""Extract structured data from this text: "{text[:200]}"

Return ONLY a JSON object with any of: email, name, phone, url
{{
    "email": "extracted_email_or_null",
    "name": "extracted_name_or_null",
    "phone": "extracted_phone_or_null",
    "url": "extracted_url_or_null"
}}"""
            
            from agents.llm_helper import call_llm
            content = call_llm(
                llm,
                messages=[
                    {"role": "system", "content": "You are a metadata extraction assistant. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=150
            )
            
            import json
            result = json.loads(content)
            # Filter out null values
            return {k: v for k, v in result.items() if v and v != 'null'}
        except Exception:
            return {}