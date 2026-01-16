"""
Data cleaning utility functions
"""
import re
from typing import Optional, Tuple, List, Dict, Any
from dateutil import parser as date_parser
from datetime import datetime


def parse_date(date_string: str, context: Optional[Dict[str, Any]] = None) -> Optional[Tuple[str, float]]:
    """
    Parse a date string and return ISO format date with confidence
    
    Args:
        date_string: Raw date string
        context: Optional context (e.g., country for date format inference)
        
    Returns:
        Tuple of (iso_date_string, confidence) or None if cannot parse
    """
    if not date_string or not isinstance(date_string, str):
        return None
    
    try:
        # Try dateutil parser first (handles many formats)
        parsed = date_parser.parse(date_string, fuzzy=True)
        iso_date = parsed.strftime('%Y-%m-%d')
        return (iso_date, 0.9)
    except (ValueError, TypeError):
        pass
    
    # Try common patterns
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),  # ISO
        (r'(\d{2})/(\d{2})/(\d{4})', '%m/%d/%Y'),  # US
        (r'(\d{2})/(\d{2})/(\d{2})', '%m/%d/%y'),  # US short
        (r'(\d{2})-(\d{2})-(\d{4})', '%m-%d-%Y'),  # US dash
    ]
    
    for pattern, fmt in patterns:
        match = re.match(pattern, date_string.strip())
        if match:
            try:
                parsed = datetime.strptime(date_string.strip(), fmt)
                iso_date = parsed.strftime('%Y-%m-%d')
                return (iso_date, 0.8)
            except ValueError:
                continue
    
    return None


def detect_phone_country(phone_string: str, context: Optional[Dict[str, Any]] = None) -> str:
    """
    Detect country from phone number or context
    
    Args:
        phone_string: Raw phone string
        context: Optional context (e.g., country column value)
        
    Returns:
        Country code (e.g., 'IN', 'US')
    """
    # Check context first (e.g., country column)
    if context:
        country = context.get('country', '').upper()
        if country:
            # Map common country names to codes
            country_map = {
                'INDIA': 'IN', 'INDIAN': 'IN', 'IN': 'IN',
                'USA': 'US', 'UNITED STATES': 'US', 'US': 'US',
                'UK': 'GB', 'UNITED KINGDOM': 'GB', 'GB': 'GB',
            }
            return country_map.get(country, 'US')
    
    # Detect from phone number format
    digits = re.sub(r'[^\d+]', '', phone_string)
    
    # Indian phone numbers: +91 or 91 prefix, or 10 digits starting with 6-9
    if digits.startswith('+91') or digits.startswith('91'):
        return 'IN'
    if len(digits) == 10 and digits[0] in '6789':
        # Could be Indian (mobile numbers start with 6-9)
        return 'IN'
    
    # US phone numbers: +1 or 1 prefix, or 10 digits
    if digits.startswith('+1') or (digits.startswith('1') and len(digits) == 11):
        return 'US'
    if len(digits) == 10 and not digits.startswith('0'):
        # Could be US (default assumption)
        return 'US'
    
    return 'US'  # Default


def normalize_phone(phone_string: str, country_code: str = None, context: Optional[Dict[str, Any]] = None) -> Optional[Tuple[str, float]]:
    """
    Normalize phone number to consistent E.164 format with country-specific formatting
    
    Args:
        phone_string: Raw phone string
        country_code: Country code (auto-detect if None)
        context: Optional context (e.g., country column value)
        
    Returns:
        Tuple of (normalized_phone, confidence) or None
    """
    if not phone_string or not isinstance(phone_string, str):
        return None
    
    # CRITICAL: If country_code is provided, NEVER auto-detect - use it directly
    # Only auto-detect if country_code is explicitly None or empty
    if not country_code or country_code.strip() == '':
        country_code = detect_phone_country(phone_string, context)
        print(f"DEBUG: normalize_phone: No country_code provided, auto-detected: '{country_code}'")
    else:
        print(f"DEBUG: normalize_phone: Using provided country_code='{country_code}' (will NOT auto-detect)")
    
    # Remove all non-digit characters except +
    digits = re.sub(r'[^\d+]', '', phone_string)
    
    # Remove common prefixes/extensions
    digits = re.sub(r'ext\.?\s*\d+', '', digits, flags=re.IGNORECASE)
    
    # CRITICAL: Use the country_code parameter to determine format (don't auto-detect from phone)
    print(f"DEBUG: normalize_phone: country_code='{country_code}', phone='{phone_string}', digits='{digits}'")
    
    # First, strip ALL country code prefixes to get the raw number
    raw_digits = digits
    # Remove any existing country code prefix (+1, +91, +44, etc.)
    if raw_digits.startswith('+'):
        # Extract just the number part (remove country code)
        if raw_digits.startswith('+91'):
            raw_digits = raw_digits[3:]
        elif raw_digits.startswith('+1'):
            raw_digits = raw_digits[2:]
        elif raw_digits.startswith('+'):
            # Generic: remove + and first 1-3 digits (country code)
            raw_digits = re.sub(r'^\+\d{1,3}', '', raw_digits)
    
    # Also remove prefix without +
    if raw_digits.startswith('91') and len(raw_digits) >= 12:
        raw_digits = raw_digits[2:]
    elif raw_digits.startswith('1') and len(raw_digits) == 11:
        raw_digits = raw_digits[1:]
    
    # Remove leading zeros
    raw_digits = raw_digits.lstrip('0')
    
    print(f"DEBUG: normalize_phone: After prefix removal, raw_digits='{raw_digits}'")
    
    # Now apply the CORRECT country code based on country_code parameter
    # CRITICAL: country_code parameter takes absolute priority - don't auto-detect from phone number
    print(f"DEBUG: normalize_phone: Applying country_code='{country_code}' to raw_digits='{raw_digits}'")
    
    # If country_code is provided, ALWAYS use it (don't fall through to auto-detection)
    if country_code and country_code.strip():
        country_code_upper = country_code.upper().strip()
        
        # Indian phone normalization: +91 followed by 10 digits (no brackets)
        if country_code_upper == 'IN':
            # Indian mobile numbers are 10 digits starting with 6-9 (or 4-9 for landlines)
            # Try to extract exactly 10 digits
            if len(raw_digits) >= 10:
                # Take last 10 digits if longer
                phone_digits = raw_digits[-10:] if len(raw_digits) > 10 else raw_digits
                # Format: +91 XXXXXXXXXX (10 digits, no brackets)
                if len(phone_digits) == 10:
                    normalized = f"+91 {phone_digits}"
                    print(f"DEBUG: normalize_phone: ✅ Indian format: '{normalized}' (from country_code='IN')")
                    return (normalized, 0.9)
            # If we have at least 8 digits, try to format anyway
            if len(raw_digits) >= 8:
                phone_digits = raw_digits[-10:] if len(raw_digits) > 10 else raw_digits.zfill(10)
                if len(phone_digits) == 10:
                    normalized = f"+91 {phone_digits}"
                    print(f"DEBUG: normalize_phone: ✅ Indian format (adjusted): '{normalized}' (from country_code='IN')")
                    return (normalized, 0.8)
            # Last resort: format with whatever digits we have
            if len(raw_digits) > 0:
                normalized = f"+91 {raw_digits}"
                print(f"DEBUG: normalize_phone: ✅ Indian format (fallback): '{normalized}' (from country_code='IN')")
                return (normalized, 0.7)
        
        # US phone normalization - ALWAYS use consistent format: +1 (XXX) XXX-XXXX
        elif country_code_upper == 'US':
            # Should be 10 digits
            if len(raw_digits) >= 10:
                # Take last 10 digits if longer
                phone_digits = raw_digits[-10:] if len(raw_digits) > 10 else raw_digits
                if len(phone_digits) == 10:
                    # Always use: +1 (XXX) XXX-XXXX format
                    normalized = f"+1 ({phone_digits[0:3]}) {phone_digits[3:6]}-{phone_digits[6:10]}"
                    print(f"DEBUG: normalize_phone: ✅ US format: '{normalized}' (from country_code='US')")
                    return (normalized, 0.9)
            # If we have at least 8 digits, try to format anyway
            if len(raw_digits) >= 8:
                phone_digits = raw_digits[-10:] if len(raw_digits) > 10 else raw_digits.zfill(10)
                if len(phone_digits) == 10:
                    normalized = f"+1 ({phone_digits[0:3]}) {phone_digits[3:6]}-{phone_digits[6:10]}"
                    print(f"DEBUG: normalize_phone: ✅ US format (adjusted): '{normalized}' (from country_code='US')")
                    return (normalized, 0.8)
            # Last resort: format with whatever digits we have
            if len(raw_digits) > 0:
                normalized = f"+1 {raw_digits}"
                print(f"DEBUG: normalize_phone: ✅ US format (fallback): '{normalized}' (from country_code='US')")
                return (normalized, 0.7)
        
        # If country_code is provided but doesn't match IN or US, use generic format with that country code
        else:
            # For other countries, use simple format: +XX XXXXXXXXX
            if len(raw_digits) >= 7:
                normalized = f"+{country_code_upper} {raw_digits}"
                print(f"DEBUG: normalize_phone: ✅ Generic format for '{country_code_upper}': '{normalized}'")
                return (normalized, 0.7)
            elif len(raw_digits) > 0:
                normalized = f"+{country_code_upper} {raw_digits}"
                print(f"DEBUG: normalize_phone: ✅ Generic format (fallback) for '{country_code_upper}': '{normalized}'")
                return (normalized, 0.6)
    
    # CRITICAL: If we reach here and country_code was provided, we should have already returned
    # This fallback should ONLY be used if country_code was NOT provided
    # If country_code was provided but we didn't format it, something went wrong - log it
    if country_code and country_code.strip():
        print(f"DEBUG: normalize_phone: ⚠️ WARNING - country_code='{country_code}' was provided but no format was applied!")
        print(f"DEBUG: normalize_phone: raw_digits='{raw_digits}', len={len(raw_digits)}")
        # Force format based on country_code even if pattern doesn't match perfectly
        country_code_upper = country_code.upper().strip()
        if country_code_upper == 'IN' and len(raw_digits) > 0:
            # Force Indian format
            phone_digits = raw_digits[-10:] if len(raw_digits) >= 10 else raw_digits.zfill(10)
            normalized = f"+91 {phone_digits}"
            print(f"DEBUG: normalize_phone: ✅ Forced Indian format: '{normalized}'")
            return (normalized, 0.7)
        elif country_code_upper == 'US' and len(raw_digits) > 0:
            # Force US format
            phone_digits = raw_digits[-10:] if len(raw_digits) >= 10 else raw_digits.zfill(10)
            if len(phone_digits) == 10:
                normalized = f"+1 ({phone_digits[0:3]}) {phone_digits[3:6]}-{phone_digits[6:10]}"
            else:
                normalized = f"+1 {phone_digits}"
            print(f"DEBUG: normalize_phone: ✅ Forced US format: '{normalized}'")
            return (normalized, 0.7)
    
    # Generic international format - but don't apply US formatting to other countries
    # This is ONLY a fallback when country_code was NOT provided
    if digits.startswith('+'):
        # For Indian numbers that weren't caught above, try again
        if digits.startswith('+91'):
            rest = digits[3:]
            if len(rest) == 10 and rest[0] in '6789':
                return (f"+91 {rest}", 0.9)
        
        # For other countries, keep simple format (don't apply US-style brackets)
        # Just ensure + prefix and proper spacing
        if len(digits) > 4:
            detected_country_code = digits[1:3] if len(digits) >= 3 else digits[1:2]
            rest = digits[3:] if len(digits) > 3 else digits[2:]
            # Simple format: +XX XXXXXXXXX (no brackets for non-US)
            if len(rest) >= 7:
                normalized = f"+{detected_country_code} {rest}"
                return (normalized, 0.7)
        return (digits, 0.7)
    
    return None


def parse_units(value_string: str) -> Optional[Tuple[float, str, float]]:
    """
    Parse a value with units (e.g., "5ft 10in", "178cm")
    
    Args:
        value_string: String with number and unit
        
    Returns:
        Tuple of (numeric_value, unit, confidence) or None
    """
    if not value_string or not isinstance(value_string, str):
        return None
    
    # Common unit patterns (order matters - more specific patterns first)
    patterns = [
        # Feet and inches formats (compound units) - HIGHEST PRIORITY
        (r'(\d+\.?\d*)\s*ft\s*(\d+\.?\d*)\s*in(?:\b|$)', 'ft_in'),  # 5ft 10in
        (r'(\d+\.?\d*)[\'\u2019]\s*(\d+\.?\d*)[\"\u201d]', 'ft_in'),  # 5'10" or 5'10"
        (r'(\d+\.?\d*)[\'\u2019]\s*(\d+\.?\d*)(?:\b|$)', 'ft_in'),  # 5'10 (apostrophe without inches mark)
        (r'(\d+\.?\d*)\s*feet\s*(\d+\.?\d*)\s*inches?(?:\b|$)', 'ft_in'),  # 5 feet 10 inches
        
        # CRITICAL: Two numbers separated by space (height in feet inches without units)
        # Must be 4-7 range for feet (realistic height) and 0-11 for inches
        (r'^(\d)\s+(\d{1,2})$', 'ft_in_implied'),  # "5 8" -> 5 feet 8 inches
        (r'^(\d)\s+(\d{1,2})\s*$', 'ft_in_implied'),  # "5 8 " with trailing space
        
        # Full word units (meters, inches, feet - must come before abbreviations)
        (r'(\d+\.?\d*)\s*meters?(?:\b|$)', 'm'),  # 1.78 meters or 1.78meters
        (r'(\d+\.?\d*)\s*inches?(?:\b|$)', 'in'),  # 70 inches or 70inches
        (r'(\d+\.?\d*)\s*feet(?:\b|$)', 'ft'),  # 5 feet or 5feet
        
        # Abbreviations (cm, m, in, ft - more flexible matching)
        (r'(\d+\.?\d*)\s*cm(?:\b|$|\s)', 'cm'),  # 178cm or 178 cm
        (r'(\d+\.?\d*)\s*m(?:\b|$|\s)', 'm'),  # 1.78m or 1.78 m
        (r'(\d+\.?\d*)\s*in(?:\b|$|\s)', 'in'),  # 70in or 70 in
        (r'(\d+\.?\d*)\s*ft(?:\b|$|\s)', 'ft'),  # 5ft or 5 ft
    ]
    
    for pattern, unit_type in patterns:
        match = re.search(pattern, value_string, re.IGNORECASE)
        if match:
            if unit_type == 'ft_in' or unit_type == 'ft_in_implied':
                # Both explicit (5ft 10in) and implied (5 8) formats
                feet = float(match.group(1))
                inches = float(match.group(2))
                
                # Validate: feet should be 3-8 (realistic height), inches should be 0-11
                if 3 <= feet <= 8 and 0 <= inches <= 11:
                    total_inches = feet * 12 + inches
                    cm = total_inches * 2.54
                    confidence = 0.9 if unit_type == 'ft_in' else 0.75  # Lower confidence for implied
                    return (cm, 'cm', confidence)
                else:
                    # Invalid feet/inches values, skip this match
                    continue
            else:
                value = float(match.group(1))
                return (value, unit_type, 0.85)
    
    return None


def convert_units(value: float, from_unit: str, to_unit: str) -> Optional[float]:
    """
    Convert between units
    
    Args:
        value: Numeric value
        from_unit: Source unit
        to_unit: Target unit
        
    Returns:
        Converted value or None
    """
    # Conversion factors to cm (base unit)
    to_cm = {
        'cm': 1.0,
        'm': 100.0,
        'in': 2.54,
        'ft': 30.48,
    }
    
    # Convert to cm first
    if from_unit not in to_cm:
        return None
    
    cm_value = value * to_cm[from_unit]
    
    # Convert from cm to target
    if to_unit not in to_cm:
        return None
    
    return cm_value / to_cm[to_unit]


def fuzzy_match_category(value: str, allowed_categories: List[str], threshold: float = 0.7) -> Optional[Tuple[str, float]]:
    """
    Fuzzy match a value to allowed categories using Levenshtein distance
    
    Args:
        value: Input value
        allowed_categories: List of allowed category values
        threshold: Minimum similarity threshold (0-1)
        
    Returns:
        Tuple of (matched_category, confidence) or None
    """
    if not value or not allowed_categories:
        return None
    
    value_lower = value.lower().strip()
    
    # Exact match
    for cat in allowed_categories:
        if cat.lower() == value_lower:
            return (cat, 1.0)
    
    # Fuzzy match using simple Levenshtein-like similarity
    best_match = None
    best_score = 0.0
    
    for cat in allowed_categories:
        cat_lower = cat.lower()
        # Simple similarity: count common characters
        similarity = _simple_similarity(value_lower, cat_lower)
        if similarity > best_score and similarity >= threshold:
            best_score = similarity
            best_match = cat
    
    if best_match:
        return (best_match, best_score)
    
    return None


def _simple_similarity(s1: str, s2: str) -> float:
    """Simple string similarity (0-1)"""
    if not s1 or not s2:
        return 0.0
    
    # Count common characters
    common = sum(1 for c in s1 if c in s2)
    max_len = max(len(s1), len(s2))
    
    if max_len == 0:
        return 1.0
    
    return common / max_len