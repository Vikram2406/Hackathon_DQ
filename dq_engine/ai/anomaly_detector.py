"""
AI-powered anomaly detection
"""
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime


class AnomalyDetector:
    """Statistical anomaly detection using historical patterns"""
    
    def __init__(self, z_score_threshold: float = 3.0):
        """
        Initialize anomaly detector
        
        Args:
            z_score_threshold: Z-score threshold for anomaly detection (default: 3.0)
        """
        self.z_score_threshold = z_score_threshold
    
    def detect(self, current_value: float, historical_values: List[float]) -> Dict[str, Any]:
        """
        Detect if current value is an anomaly
        
        Args:
            current_value: Current metric value
            historical_values: List of historical values
        
        Returns:
            Dictionary with anomaly detection results
        """
        if not historical_values or len(historical_values) < 3:
            return {
                'is_anomaly': False,
                'severity': 'UNKNOWN',
                'message': 'Insufficient historical data for anomaly detection',
                'z_score': 0
            }
        
        mean = np.mean(historical_values)
        std = np.std(historical_values)
        
        # Calculate Z-score
        z_score = (current_value - mean) / std if std > 0 else 0
        
        # Determine if anomaly
        is_anomaly = abs(z_score) > self.z_score_threshold
        
        # Calculate severity
        severity = self._calculate_severity(z_score)
        
        return {
            'is_anomaly': is_anomaly,
            'severity': severity,
            'z_score': round(float(z_score), 2),
            'expected_value': round(float(mean), 2),
            'std_deviation': round(float(std), 2),
            'deviation_pct': round(abs((current_value - mean) / mean * 100), 2) if mean != 0 else 0,
            'threshold': self.z_score_threshold
        }
    
    def _calculate_severity(self, z_score: float) -> str:
        """Calculate severity level based on Z-score"""
        abs_z = abs(z_score)
        
        if abs_z > 5:
            return 'CRITICAL'
        elif abs_z > 3:
            return 'HIGH'
        elif abs_z > 2:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def detect_multiple(self, metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect anomalies across multiple metrics
        
        Args:
            metrics: Dictionary of metric_name -> {'current': value, 'historical': [values]}
        
        Returns:
            Dictionary with anomaly results for all metrics
        """
        results = {}
        anomalies_found = []
        
        for metric_name, data in metrics.items():
            current = data.get('current')
            historical = data.get('historical', [])
            
            if current is None:
                continue
            
            detection = self.detect(current, historical)
            results[metric_name] = detection
            
            if detection['is_anomaly']:
                anomalies_found.append({
                    'metric': metric_name,
                    'severity': detection['severity'],
                    'z_score': detection['z_score']
                })
        
        # Overall assessment
        if not anomalies_found:
            overall_severity = 'NONE'
        else:
            severities = [a['severity'] for a in anomalies_found]
            if 'CRITICAL' in severities:
                overall_severity = 'CRITICAL'
            elif 'HIGH' in severities:
                overall_severity = 'HIGH'
            elif 'MEDIUM' in severities:
                overall_severity = 'MEDIUM'
            else:
                overall_severity = 'LOW'
        
        return {
            'metrics': results,
            'anomalies_found': anomalies_found,
            'overall_severity': overall_severity,
            'total_anomalies': len(anomalies_found)
        }
