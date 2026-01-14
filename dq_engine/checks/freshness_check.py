"""
Data freshness check implementation
"""
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timedelta


def check_freshness(
    df: pd.DataFrame,
    timestamp_column: str,
    max_age_hours: float = 24,
    datetime_format: Optional[str] = None
) -> Dict[str, Any]:
    """
    Check if data is fresh based on timestamp column
    
    Args:
        df: DataFrame to check
        timestamp_column: Name of the timestamp column
        max_age_hours: Maximum acceptable age in hours
        datetime_format: Format string for parsing timestamps (if needed)
    
    Returns:
        Dictionary with freshness check results
    """
    if timestamp_column not in df.columns:
        return {
            'check_type': 'freshness_check',
            'status': 'ERROR',
            'error': f'Timestamp column {timestamp_column} not found'
        }
    
    try:
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
            if datetime_format:
                timestamps = pd.to_datetime(df[timestamp_column], format=datetime_format)
            else:
                timestamps = pd.to_datetime(df[timestamp_column])
        else:
            timestamps = df[timestamp_column]
        
        # Get latest timestamp
        latest_timestamp = timestamps.max()
        oldest_timestamp = timestamps.min()
        
        # Calculate age
        current_time = datetime.now()
        if latest_timestamp.tzinfo is not None:
            # Make current_time timezone-aware if latest_timestamp is
            from datetime import timezone
            current_time = current_time.replace(tzinfo=timezone.utc)
        
        age = current_time - latest_timestamp
        age_hours = age.total_seconds() / 3600
        
        # Determine status
        status = 'PASS' if age_hours <= max_age_hours else 'FAIL'
        
        results = {
            'check_type': 'freshness_check',
            'status': status,
            'timestamp_column': timestamp_column,
            'latest_timestamp': str(latest_timestamp),
            'oldest_timestamp': str(oldest_timestamp),
            'current_time': str(current_time),
            'age_hours': round(age_hours, 2),
            'max_age_hours': max_age_hours,
            'is_fresh': age_hours <= max_age_hours,
            'total_records': len(df)
        }
        
        return results
        
    except Exception as e:
        return {
            'check_type': 'freshness_check',
            'status': 'ERROR',
            'error': f'Failed to check freshness: {str(e)}'
        }


def check_data_gaps(
    df: pd.DataFrame,
    timestamp_column: str,
    expected_frequency: str = 'D',
    tolerance_hours: float = 2
) -> Dict[str, Any]:
    """
    Check for gaps in data based on expected frequency
    
    Args:
        df: DataFrame to check
        timestamp_column: Name of the timestamp column
        expected_frequency: Expected frequency ('H'=hourly, 'D'=daily, 'W'=weekly)
        tolerance_hours: Tolerance for gaps in hours
    
    Returns:
        Dictionary with gap detection results
    """
    if timestamp_column not in df.columns:
        return {
            'check_type': 'data_gap_check',
            'status': 'ERROR',
            'error': f'Timestamp column {timestamp_column} not found'
        }
    
    try:
        # Convert to datetime
        timestamps = pd.to_datetime(df[timestamp_column])
        timestamps_sorted = timestamps.sort_values()
        
        # Calculate time differences
        time_diffs = timestamps_sorted.diff()
        
        # Expected interval based on frequency
        freq_map = {
            'H': timedelta(hours=1),
            'D': timedelta(days=1),
            'W': timedelta(weeks=1)
        }
        expected_interval = freq_map.get(expected_frequency, timedelta(days=1))
        tolerance = timedelta(hours=tolerance_hours)
        
        # Find gaps
        gaps = time_diffs[time_diffs > (expected_interval + tolerance)]
        
        results = {
            'check_type': 'data_gap_check',
            'status': 'PASS' if len(gaps) == 0 else 'FAIL',
            'expected_frequency': expected_frequency,
            'gaps_found': len(gaps),
            'largest_gap_hours': round(gaps.max().total_seconds() / 3600, 2) if len(gaps) > 0 else 0,
            'total_records': len(df)
        }
        
        return results
        
    except Exception as e:
        return {
            'check_type': 'data_gap_check',
            'status': 'ERROR',
            'error': f'Failed to check data gaps: {str(e)}'
        }
