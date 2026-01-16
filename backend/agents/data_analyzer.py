"""
Data Analyzer - Analyzes dataset to learn patterns and make data-driven decisions
"""
from typing import List, Dict, Any, Optional, Set
import re
from collections import Counter


class DataAnalyzer:
    """Analyzes dataset patterns to help agents make data-driven decisions"""
    
    @staticmethod
    def analyze_column_types(dataset_rows: List[Dict[str, Any]], sample_size: int = 1000) -> Dict[str, Dict[str, Any]]:
        """
        Analyze each column to determine its type and patterns
        
        Returns:
            Dict mapping column_name -> {
                'type': 'email', 'phone', 'date', 'company', 'city', 'country', 'numeric', 'text', etc.
                'patterns': {...},
                'most_common': value,
                'unique_count': int,
                'sample_values': [...]
            }
        """
        if not dataset_rows:
            return {}
        
        columns = list(dataset_rows[0].keys())
        column_analysis = {}
        sample = dataset_rows[:sample_size]
        
        for col in columns:
            values = [str(row.get(col, '')).strip() for row in sample if row.get(col)]
            if not values:
                continue
            
            analysis = {
                'type': 'text',
                'patterns': {},
                'most_common': None,
                'unique_count': len(set(values)),
                'sample_values': values[:10],
                'non_null_count': len(values)
            }
            
            # Detect email patterns
            email_count = sum(1 for v in values if '@' in v and re.search(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', v))
            if email_count > len(values) * 0.5:  # >50% are emails
                analysis['type'] = 'email'
                # Find most common email domain
                domains = [v.split('@')[1].split('.')[0] if '@' in v else None for v in values if '@' in v]
                domain_counter = Counter([d for d in domains if d])
                if domain_counter:
                    analysis['most_common_domain'] = domain_counter.most_common(1)[0][0]
            
            # Detect phone patterns
            phone_patterns = [
                r'\+?\d{10,}',  # 10+ digits
                r'\+91',  # Indian
                r'\+1',   # US
            ]
            phone_count = sum(1 for v in values if any(re.search(p, v) for p in phone_patterns))
            if phone_count > len(values) * 0.3:  # >30% are phones
                analysis['type'] = 'phone'
                # Detect country from phone patterns
                if any('+91' in v or re.search(r'^91\d{10}', re.sub(r'[^\d+]', '', v)) for v in values):
                    analysis['country'] = 'IN'
                elif any('+1' in v or re.search(r'^1\d{10}', re.sub(r'[^\d+]', '', v)) for v in values):
                    analysis['country'] = 'US'
            
            # Detect date patterns
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # ISO
                r'\d{2}/\d{2}/\d{4}',  # US
                r'\d{2}-\d{2}-\d{4}',  # US dash
            ]
            date_count = sum(1 for v in values if any(re.search(p, v) for p in date_patterns))
            if date_count > len(values) * 0.3:
                analysis['type'] = 'date'
            
            # Detect numeric patterns
            numeric_count = sum(1 for v in values if re.match(r'^\d+\.?\d*$', v))
            if numeric_count > len(values) * 0.7:
                analysis['type'] = 'numeric'
            
            # Find most common value
            value_counter = Counter(values)
            if value_counter:
                analysis['most_common'] = value_counter.most_common(1)[0][0]
                analysis['most_common_count'] = value_counter.most_common(1)[0][1]
            
            column_analysis[col] = analysis
        
        return column_analysis
    
    @staticmethod
    def find_related_columns(dataset_rows: List[Dict[str, Any]], target_type: str) -> Dict[str, List[str]]:
        """
        Find columns that are related (e.g., city/country, birth_date/job_start_date)
        
        Returns:
            Dict mapping relationship_type -> [column_names]
        """
        if not dataset_rows:
            return {}
        
        column_types = DataAnalyzer.analyze_column_types(dataset_rows)
        relationships = {
            'date_pairs': [],
            'location_pairs': [],
            'email_phone_pairs': []
        }
        
        columns = list(column_types.keys())
        
        # Find date pairs (birth/job start, start/end, etc.)
        date_cols = [col for col, info in column_types.items() if info['type'] == 'date']
        if len(date_cols) >= 2:
            relationships['date_pairs'] = date_cols
        
        # Find location pairs (city/country, state/country)
        location_keywords = {
            'city': ['city', 'town', 'location'],
            'state': ['state', 'province', 'region'],
            'country': ['country', 'nation']
        }
        
        location_cols = {}
        for col in columns:
            col_lower = col.lower()
            for loc_type, keywords in location_keywords.items():
                if any(kw in col_lower for kw in keywords):
                    if loc_type not in location_cols:
                        location_cols[loc_type] = []
                    location_cols[loc_type].append(col)
        
        if location_cols:
            relationships['location_pairs'] = location_cols
        
        return relationships
    
    @staticmethod
    def get_data_context(dataset_rows: List[Dict[str, Any]], column: str, sample_size: int = 100) -> str:
        """
        Get context about a column's data for LLM prompts
        
        Returns:
            String describing the column's data patterns
        """
        if not dataset_rows:
            return ""
        
        sample = dataset_rows[:sample_size]
        values = [str(row.get(column, '')).strip() for row in sample if row.get(column)]
        
        if not values:
            return f"Column '{column}' has no values"
        
        unique_values = list(set(values))
        most_common = Counter(values).most_common(5)
        
        context = f"Column '{column}' has {len(values)} values, {len(unique_values)} unique. "
        context += f"Most common values: {', '.join([f'{v}({c})' for v, c in most_common[:3]])}. "
        
        if len(unique_values) <= 20:
            context += f"All unique values: {', '.join(unique_values[:10])}"
        
        return context
    
    @staticmethod
    def detect_email_domains(dataset_rows: List[Dict[str, Any]], email_column: str) -> Dict[str, int]:
        """Detect most common email domains in the data"""
        if not dataset_rows:
            return {}
        
        domains = []
        for row in dataset_rows:
            email = str(row.get(email_column, '')).strip()
            if '@' in email:
                domain = email.split('@')[1].lower()
                domains.append(domain)
        
        domain_counter = Counter(domains)
        return dict(domain_counter.most_common(10))
    
    @staticmethod
    def detect_phone_country_from_data(dataset_rows: List[Dict[str, Any]], phone_column: str, country_column: Optional[str] = None) -> str:
        """Detect country from phone numbers and country column in the data"""
        if not dataset_rows:
            return 'US'  # Default
        
        # First check country column if available
        if country_column:
            countries = [str(row.get(country_column, '')).upper() for row in dataset_rows if row.get(country_column)]
            country_counter = Counter(countries)
            if country_counter:
                most_common_country = country_counter.most_common(1)[0][0]
                country_map = {
                    'INDIA': 'IN', 'INDIAN': 'IN', 'IN': 'IN',
                    'USA': 'US', 'UNITED STATES': 'US', 'US': 'US',
                }
                if most_common_country in country_map:
                    return country_map[most_common_country]
        
        # Detect from phone number patterns
        phone_patterns = {
            'IN': [r'\+91', r'^91\d{10}', r'^\d{10}$'],  # Indian patterns
            'US': [r'\+1', r'^1\d{10}']  # US patterns
        }
        
        country_scores = {'IN': 0, 'US': 0}
        for row in dataset_rows[:1000]:  # Sample
            phone = str(row.get(phone_column, '')).strip()
            digits = re.sub(r'[^\d+]', '', phone)
            
            for country, patterns in phone_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, phone) or re.search(pattern, digits):
                        country_scores[country] += 1
                        break
        
        # Return country with highest score, default to US
        if country_scores['IN'] > country_scores['US']:
            return 'IN'
        return 'US'
