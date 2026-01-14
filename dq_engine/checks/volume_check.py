"""
Volume anomaly check implementation
"""
import pandas as pd
from typing import Dict, Any, List, Optional
import numpy as np


def check_volume(
    current_count: int,
    historical_counts: List[int],
    threshold_pct: float = 20.0,
    use_statistical: bool = True
) -> Dict[str, Any]:
    """
    Check if current row count is anomalous compared to historical data
    
    Args:
        current_count: Current row count
        historical_counts: List of historical row counts
        threshold_pct: Percentage deviation threshold (if not using statistical)
        use_statistical: If True, use Z-score; if False, use percentage threshold
    
    Returns:
        Dictionary with volume check results
    """
    if not historical_counts:
        return {
            'check_type': 'volume_check',
            'status': 'WARNING',
            'message': 'No historical data available for comparison',
            'current_count': current_count
        }
    
    historical_avg = np.mean(historical_counts)
    historical_std = np.std(historical_counts)
    historical_min = np.min(historical_counts)
    historical_max = np.max(historical_counts)
    
    # Calculate deviation
    deviation = current_count - historical_avg
    deviation_pct = (deviation / historical_avg) * 100 if historical_avg > 0 else 0
    
    # Determine status
    if use_statistical and len(historical_counts) >= 3:
        # Use Z-score for statistical anomaly detection
        z_score = (current_count - historical_avg) / historical_std if historical_std > 0 else 0
        is_anomaly = abs(z_score) > 3  # 3 standard deviations
        status = 'FAIL' if is_anomaly else 'PASS'
        
        results = {
            'check_type': 'volume_check',
            'status': status,
            'method': 'statistical',
            'current_count': current_count,
            'expected_count': round(historical_avg, 2),
            'historical_min': historical_min,
            'historical_max': historical_max,
            'historical_std': round(historical_std, 2),
            'deviation': int(deviation),
            'deviation_pct': round(deviation_pct, 2),
            'z_score': round(z_score, 2),
            'is_anomaly': is_anomaly,
            'threshold_z_score': 3
        }
    else:
        # Use percentage threshold
        is_anomaly = abs(deviation_pct) > threshold_pct
        status = 'FAIL' if is_anomaly else 'PASS'
        
        results = {
            'check_type': 'volume_check',
            'status': status,
            'method': 'threshold',
            'current_count': current_count,
            'expected_count': round(historical_avg, 2),
            'historical_min': historical_min,
            'historical_max': historical_max,
            'deviation': int(deviation),
            'deviation_pct': round(deviation_pct, 2),
            'is_anomaly': is_anomaly,
            'threshold_pct': threshold_pct
        }
    
    return results


def check_volume_from_df(
    df: pd.DataFrame,
    historical_counts: List[int],
    threshold_pct: float = 20.0,
    use_statistical: bool = True
) -> Dict[str, Any]:
    """
    Check volume anomaly for a DataFrame
    
    Args:
        df: DataFrame to check
        historical_counts: List of historical row counts
        threshold_pct: Percentage deviation threshold
        use_statistical: If True, use Z-score; if False, use percentage threshold
    
    Returns:
        Dictionary with volume check results
    """
    current_count = len(df)
    return check_volume(current_count, historical_counts, threshold_pct, use_statistical)


def calculate_volume_trend(counts: List[int], window: int = 7) -> Dict[str, Any]:
    """
    Calculate volume trend over time
    
    Args:
        counts: List of row counts (chronological order)
        window: Window size for moving average
    
    Returns:
        Dictionary with trend analysis
    """
    if len(counts) < window:
        return {
            'trend': 'insufficient_data',
            'message': f'Need at least {window} data points for trend analysis'
        }
    
    # Calculate moving average
    moving_avg = pd.Series(counts).rolling(window=window).mean().tolist()
    
    # Determine trend direction
    recent_avg = np.mean(counts[-window:])
    previous_avg = np.mean(counts[-2*window:-window]) if len(counts) >= 2*window else np.mean(counts[:-window])
    
    trend_pct = ((recent_avg - previous_avg) / previous_avg) * 100 if previous_avg > 0 else 0
    
    if trend_pct > 10:
        trend = 'increasing'
    elif trend_pct < -10:
        trend = 'decreasing'
    else:
        trend = 'stable'
    
    return {
        'trend': trend,
        'trend_pct': round(trend_pct, 2),
        'recent_avg': round(recent_avg, 2),
        'previous_avg': round(previous_avg, 2),
        'moving_average': [round(x, 2) if not np.isnan(x) else None for x in moving_avg]
    }
