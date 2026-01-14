"""
Data Quality Validation DAG for S3 Data Sources
This DAG runs comprehensive data quality checks on S3 datasets
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from backend.connectors.s3_connector import S3Connector
from dq_engine.checks.null_check import check_nulls
from dq_engine.checks.duplicate_check import check_duplicates
from dq_engine.checks.freshness_check import check_freshness
from dq_engine.checks.volume_check import check_volume
from dq_engine.ai.anomaly_detector import AnomalyDetector
from dq_engine.ai.explainer import AIExplainer
from backend.database import get_db
from backend.models.database import QualityMetric, AnomalyRecord, DatasetConfig
import pandas as pd
import uuid
from sqlalchemy.orm import Session

# Default arguments
default_args = {
    'owner': 'dq_platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'dq_validation_s3',
    default_args=default_args,
    description='Data Quality Validation for S3 Data Sources',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['data_quality', 's3', 'validation'],
)


def load_data_from_s3(**context):
    """
    Task 1: Load data from S3
    """
    print("=" * 80)
    print("TASK 1: Loading data from S3")
    print("=" * 80)
    
    # Get dataset configuration from context or use default
    dataset_id = context.get('dag_run').conf.get('dataset_id') if context.get('dag_run') and context.get('dag_run').conf else None
    
    # Default S3 configuration
    s3_config = {
        'bucket': 'hackathon-dq',
        'key': 'customers-10000.csv',
        'file_format': 'csv',
        'region': 'ap-south-1'
    }
    
    # If dataset_id provided, fetch from database
    if dataset_id:
        db = next(get_db())
        try:
            dataset = db.query(DatasetConfig).filter(DatasetConfig.id == dataset_id).first()
            if dataset and dataset.source_type == 's3':
                s3_config = dataset.connection_details
        finally:
            db.close()
    
    print(f"S3 Configuration:")
    print(f"  Bucket: {s3_config['bucket']}")
    print(f"  Key: {s3_config['key']}")
    print(f"  Region: {s3_config.get('region', 'us-east-1')}")
    
    # Connect to S3 and load data
    connector = S3Connector(s3_config)
    connector.connect()
    
    if not connector.test_connection():
        raise Exception("Failed to connect to S3")
    
    # Read data
    df = connector.read_data()
    print(f"\n✅ Successfully loaded {len(df)} rows from S3")
    print(f"Columns: {list(df.columns)}")
    print(f"\nFirst 5 rows:")
    print(df.head())
    
    # Store metadata for downstream tasks
    context['task_instance'].xcom_push(key='row_count', value=len(df))
    context['task_instance'].xcom_push(key='columns', value=list(df.columns))
    context['task_instance'].xcom_push(key='dataset_id', value=dataset_id or 'default_s3')
    context['task_instance'].xcom_push(key='s3_config', value=s3_config)
    
    # Save data to temporary location for other tasks
    temp_file = f"/tmp/dq_data_{context['run_id']}.csv"
    df.to_csv(temp_file, index=False)
    context['task_instance'].xcom_push(key='temp_file', value=temp_file)
    
    return f"Loaded {len(df)} rows successfully"


def run_null_check(**context):
    """
    Task 2: Run Null Value Check
    """
    print("=" * 80)
    print("TASK 2: Running Null Check")
    print("=" * 80)
    
    # Load data
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='load_s3_data')
    df = pd.read_csv(temp_file)
    
    # Get dataset info
    dataset_id = context['task_instance'].xcom_pull(key='dataset_id', task_ids='load_s3_data')
    run_id = context['run_id']
    
    # Run null check
    result = check_nulls(df, columns=list(df.columns))
    result['status'] = result.get('status', 'PASS')
    result['details'] = result.get('summary', {})
    
    print(f"\nNull Check Result: {result['status']}")
    print(f"Details: {result['details']}")
    
    # Store result in database
    db = next(get_db())
    try:
        metric = QualityMetric(
            dataset_id=dataset_id,
            run_id=run_id,
            check_type='null_check',
            status=result['status'],
            value=result.get('total_nulls', 0),
            details=result['details'],
            timestamp=datetime.utcnow()
        )
        db.add(metric)
        db.commit()
        print("✅ Null check results saved to database")
    finally:
        db.close()
    
    context['task_instance'].xcom_push(key='null_check_status', value=result['status'])
    return result['status']


def run_duplicate_check(**context):
    """
    Task 3: Run Duplicate Check
    """
    print("=" * 80)
    print("TASK 3: Running Duplicate Check")
    print("=" * 80)
    
    # Load data
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='load_s3_data')
    df = pd.read_csv(temp_file)
    
    # Get dataset info
    dataset_id = context['task_instance'].xcom_pull(key='dataset_id', task_ids='load_s3_data')
    run_id = context['run_id']
    
    # Detect primary key (first column or ID-like column)
    columns = df.columns
    primary_key = None
    for col in columns:
        if 'id' in col.lower():
            primary_key = col
            break
    
    if not primary_key and len(columns) > 0:
        primary_key = columns[0]
    
    print(f"Using primary key: {primary_key}")
    
    # Run duplicate check
    result = check_duplicates(df, primary_key=[primary_key])
    result['details'] = {
        'duplicate_count': result.get('duplicate_count', 0),
        'duplicate_percentage': result.get('duplicate_percentage', 0),
        'primary_key': primary_key
    }
    
    print(f"\nDuplicate Check Result: {result['status']}")
    print(f"Details: {result['details']}")
    
    # Store result in database
    db = next(get_db())
    try:
        metric = QualityMetric(
            dataset_id=dataset_id,
            run_id=run_id,
            check_type='duplicate_check',
            status=result['status'],
            value=result['details'].get('duplicate_count', 0),
            details=result['details'],
            timestamp=datetime.utcnow()
        )
        db.add(metric)
        db.commit()
        print("✅ Duplicate check results saved to database")
    finally:
        db.close()
    
    context['task_instance'].xcom_push(key='dup_check_status', value=result['status'])
    return result['status']


def run_freshness_check(**context):
    """
    Task 4: Run Freshness Check
    """
    print("=" * 80)
    print("TASK 4: Running Freshness Check")
    print("=" * 80)
    
    # Load data
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='load_s3_data')
    df = pd.read_csv(temp_file)
    
    # Get dataset info
    dataset_id = context['task_instance'].xcom_pull(key='dataset_id', task_ids='load_s3_data')
    run_id = context['run_id']
    
    # Find timestamp column
    timestamp_col = None
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'updated', 'timestamp']):
            timestamp_col = col
            break
    
    if timestamp_col:
        print(f"Using timestamp column: {timestamp_col}")
        result = check_freshness(df, timestamp_column=timestamp_col, max_age_hours=24)
        result['details'] = result.copy()
    else:
        print("⚠️ No timestamp column found, skipping freshness check")
        result = {
            'status': 'SKIP',
            'details': {'message': 'No timestamp column found in dataset'}
        }
    
    print(f"\nFreshness Check Result: {result['status']}")
    print(f"Details: {result['details']}")
    
    # Store result in database
    db = next(get_db())
    try:
        metric = QualityMetric(
            dataset_id=dataset_id,
            run_id=run_id,
            check_type='freshness_check',
            status=result['status'],
            value=result['details'].get('age_hours', 0),
            details=result['details'],
            timestamp=datetime.utcnow()
        )
        db.add(metric)
        db.commit()
        print("✅ Freshness check results saved to database")
    finally:
        db.close()
    
    return result['status']


def run_volume_check(**context):
    """
    Task 5: Run Volume Anomaly Check
    """
    print("=" * 80)
    print("TASK 5: Running Volume Check")
    print("=" * 80)
    
    # Load data
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='load_s3_data')
    df = pd.read_csv(temp_file)
    
    # Get dataset info
    dataset_id = context['task_instance'].xcom_pull(key='dataset_id', task_ids='load_s3_data')
    run_id = context['run_id']
    
    current_count = len(df)
    
    # Get historical counts from database
    db = next(get_db())
    try:
        historical_metrics = db.query(QualityMetric).filter(
            QualityMetric.dataset_id == dataset_id,
            QualityMetric.check_type == 'volume_check'
        ).order_by(QualityMetric.timestamp.desc()).limit(30).all()
        
        historical_counts = [float(m.value) for m in historical_metrics if m.value is not None]
    finally:
        db.close()
    
    print(f"Current row count: {current_count}")
    print(f"Historical data points: {len(historical_counts)}")
    
    # Run volume check
    result = check_volume(
        current_count=current_count,
        historical_counts=historical_counts,
        threshold_pct=20
    )
    result['details'] = result.copy()
    
    print(f"\nVolume Check Result: {result['status']}")
    print(f"Details: {result['details']}")
    
    # Store result in database
    db = next(get_db())
    try:
        metric = QualityMetric(
            dataset_id=dataset_id,
            run_id=run_id,
            check_type='volume_check',
            status=result['status'],
            value=current_count,
            details=result['details'],
            timestamp=datetime.utcnow()
        )
        db.add(metric)
        db.commit()
        print("✅ Volume check results saved to database")
    finally:
        db.close()
    
    context['task_instance'].xcom_push(key='volume_check_status', value=result['status'])
    return result['status']


def run_ai_analysis(**context):
    """
    Task 6: Run AI Analysis for Anomalies
    """
    print("=" * 80)
    print("TASK 6: Running AI Analysis")
    print("=" * 80)
    
    # Get all check results
    null_status = context['task_instance'].xcom_pull(key='null_check_status', task_ids='null_check')
    dup_status = context['task_instance'].xcom_pull(key='dup_check_status', task_ids='duplicate_check')
    volume_status = context['task_instance'].xcom_pull(key='volume_check_status', task_ids='volume_check')
    
    dataset_id = context['task_instance'].xcom_pull(key='dataset_id', task_ids='load_s3_data')
    run_id = context['run_id']
    
    print(f"Check Results Summary:")
    print(f"  Null Check: {null_status}")
    print(f"  Duplicate Check: {dup_status}")
    print(f"  Volume Check: {volume_status}")
    
    # Determine if AI analysis is needed
    failed_checks = [
        check for check, status in [
            ('Null Check', null_status),
            ('Duplicate Check', dup_status),
            ('Volume Check', volume_status)
        ] if status == 'FAIL'
    ]
    
    if failed_checks:
        print(f"\n🔍 {len(failed_checks)} check(s) failed. Running AI analysis...")
        
        # Get OpenAI API key
        openai_key = os.getenv('OPENAI_API_KEY')
        
        if openai_key and not openai_key.startswith('your_'):
            try:
                # Initialize AI explainer
                explainer = AIExplainer(api_key=openai_key)
                
                # Get failure context
                db = next(get_db())
                try:
                    metrics = db.query(QualityMetric).filter(
                        QualityMetric.dataset_id == dataset_id,
                        QualityMetric.run_id == run_id,
                        QualityMetric.status == 'FAIL'
                    ).all()
                    
                    failures = [
                        {
                            'check_type': m.check_type,
                            'details': m.details
                        }
                        for m in metrics
                    ]
                    
                    # Generate AI explanation
                    explanation = explainer.explain_failures(failures, dataset_name='S3 Dataset')
                    
                    print(f"\n🤖 AI Analysis:")
                    print(explanation['explanation'])
                    
                    # Determine severity
                    severity = 'HIGH' if len(failed_checks) > 2 else 'MEDIUM'
                    
                    # Store anomaly record
                    anomaly = AnomalyRecord(
                        dataset_id=dataset_id,
                        metric_type='multiple_failures',
                        severity=severity,
                        risk_level=severity,
                        ai_explanation=explanation['explanation'],
                        recommended_actions=explanation.get('recommendations', []),
                        metadata={'failed_checks': failed_checks, 'run_id': run_id},
                        detected_at=datetime.utcnow()
                    )
                    db.add(anomaly)
                    db.commit()
                    
                    print("✅ AI analysis saved to database")
                    
                finally:
                    db.close()
                    
            except Exception as e:
                print(f"⚠️ AI analysis failed: {str(e)}")
        else:
            print("⚠️ OpenAI API key not configured, skipping AI analysis")
    else:
        print("\n✅ All checks passed! No AI analysis needed.")
    
    return "AI analysis complete"


def cleanup_temp_files(**context):
    """
    Task 7: Cleanup temporary files
    """
    print("=" * 80)
    print("TASK 7: Cleanup")
    print("=" * 80)
    
    temp_file = context['task_instance'].xcom_pull(key='temp_file', task_ids='load_s3_data')
    
    if temp_file and os.path.exists(temp_file):
        os.remove(temp_file)
        print(f"✅ Cleaned up temporary file: {temp_file}")
    
    print("\n" + "=" * 80)
    print("DATA QUALITY VALIDATION COMPLETE")
    print("=" * 80)
    
    return "Cleanup complete"


# Define tasks
load_task = PythonOperator(
    task_id='load_s3_data',
    python_callable=load_data_from_s3,
    dag=dag,
)

null_check_task = PythonOperator(
    task_id='null_check',
    python_callable=run_null_check,
    dag=dag,
)

duplicate_check_task = PythonOperator(
    task_id='duplicate_check',
    python_callable=run_duplicate_check,
    dag=dag,
)

freshness_check_task = PythonOperator(
    task_id='freshness_check',
    python_callable=run_freshness_check,
    dag=dag,
)

volume_check_task = PythonOperator(
    task_id='volume_check',
    python_callable=run_volume_check,
    dag=dag,
)

ai_analysis_task = PythonOperator(
    task_id='ai_analysis',
    python_callable=run_ai_analysis,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_temp_files,
    dag=dag,
)

# Define task dependencies
# Load data first, then run all checks in parallel, then AI analysis, then cleanup
load_task >> [null_check_task, duplicate_check_task, freshness_check_task, volume_check_task]
[null_check_task, duplicate_check_task, freshness_check_task, volume_check_task] >> ai_analysis_task
ai_analysis_task >> cleanup_task
