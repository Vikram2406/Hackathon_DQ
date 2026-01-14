"""
Null value check implementation
"""
import pandas as pd
from typing import List, Dict, Any


def check_nulls(df: pd.DataFrame, columns: List[str] = None) -> Dict[str, Any]:
    """
    Check for null values in specified columns
    
    Args:
        df: DataFrame to check
        columns: List of columns to check (if None, check all columns)
    
    Returns:
        Dictionary with null check results
    """
    if columns is None:
        columns = df.columns.tolist()
    
    results = {
        'check_type': 'null_check',
        'status': 'PASS',
        'total_rows': len(df),
        'columns_checked': len(columns),
        'column_results': {},
        'summary': {}
    }
    
    total_nulls = 0
    failed_columns = []
    
    for col in columns:
        if col not in df.columns:
            results['column_results'][col] = {
                'error': f'Column {col} not found in DataFrame'
            }
            continue
        
        null_count = int(df[col].isnull().sum())
        null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
        
        col_status = 'PASS' if null_count == 0 else 'FAIL'
        if col_status == 'FAIL':
            failed_columns.append(col)
            results['status'] = 'FAIL'
        
        results['column_results'][col] = {
            'null_count': null_count,
            'null_percentage': round(null_pct, 2),
            'status': col_status
        }
        
        total_nulls += null_count
    
    # Summary
    results['summary'] = {
        'total_nulls': total_nulls,
        'total_null_percentage': round((total_nulls / (len(df) * len(columns))) * 100, 2) if len(df) > 0 else 0,
        'failed_columns': failed_columns,
        'failed_column_count': len(failed_columns)
    }
    
    return results


def check_nulls_required(df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
    """
    Check for null values in required columns (stricter version)
    
    Args:
        df: DataFrame to check
        required_columns: List of columns that must not have nulls
    
    Returns:
        Dictionary with null check results
    """
    return check_nulls(df, required_columns)
