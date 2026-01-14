"""
Duplicate record check implementation
"""
import pandas as pd
from typing import List, Dict, Any, Optional


def check_duplicates(df: pd.DataFrame, primary_key: List[str], return_duplicates: bool = False) -> Dict[str, Any]:
    """
    Check for duplicate records based on primary key
    
    Args:
        df: DataFrame to check
        primary_key: List of columns that form the primary key
        return_duplicates: If True, return sample duplicate records
    
    Returns:
        Dictionary with duplicate check results
    """
    # Validate primary key columns exist
    missing_cols = [col for col in primary_key if col not in df.columns]
    if missing_cols:
        return {
            'check_type': 'duplicate_check',
            'status': 'ERROR',
            'error': f'Primary key columns not found: {missing_cols}'
        }
    
    # Find duplicates
    duplicate_mask = df.duplicated(subset=primary_key, keep=False)
    duplicate_count = duplicate_mask.sum()
    unique_duplicate_keys = df[duplicate_mask].duplicated(subset=primary_key, keep='first').sum()
    
    total_rows = len(df)
    duplicate_pct = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
    
    results = {
        'check_type': 'duplicate_check',
        'status': 'PASS' if duplicate_count == 0 else 'FAIL',
        'total_rows': total_rows,
        'duplicate_count': int(duplicate_count),
        'unique_duplicate_keys': int(unique_duplicate_keys),
        'duplicate_percentage': round(duplicate_pct, 2),
        'primary_key': primary_key
    }
    
    # Add sample duplicates if requested
    if return_duplicates and duplicate_count > 0:
        duplicate_samples = df[duplicate_mask].head(10)[primary_key].to_dict('records')
        results['sample_duplicates'] = duplicate_samples
    
    return results


def find_duplicate_groups(df: pd.DataFrame, primary_key: List[str], max_groups: int = 5) -> Dict[str, Any]:
    """
    Find groups of duplicate records
    
    Args:
        df: DataFrame to check
        primary_key: List of columns that form the primary key
        max_groups: Maximum number of duplicate groups to return
    
    Returns:
        Dictionary with duplicate groups
    """
    duplicate_mask = df.duplicated(subset=primary_key, keep=False)
    
    if not duplicate_mask.any():
        return {
            'duplicate_groups': [],
            'total_groups': 0
        }
    
    # Get duplicate records
    duplicates = df[duplicate_mask].copy()
    
    # Group by primary key
    groups = []
    for key_values, group in duplicates.groupby(primary_key):
        if len(groups) >= max_groups:
            break
        
        groups.append({
            'key': dict(zip(primary_key, key_values)) if len(primary_key) > 1 else {primary_key[0]: key_values},
            'count': len(group),
            'sample_records': group.head(3).to_dict('records')
        })
    
    return {
        'duplicate_groups': groups,
        'total_groups': len(duplicates.groupby(primary_key))
    }
