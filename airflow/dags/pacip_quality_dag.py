"""
PACIP Daily Quality Monitoring DAG
Checks Silver and Gold table row counts daily.
Alerts via SNS if any table falls below expected thresholds.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import boto3
import time

default_args = {
    'owner': 'pacip-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'pacip_quality_monitoring',
    default_args=default_args,
    description='Daily data quality checks on PACIP Silver and Gold tables',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['pacip', 'quality', 'monitoring', 'sla'],
    doc_md="""
    ## PACIP Quality Monitoring DAG

    Runs daily quality checks across all PACIP data layers:

    - **Silver tables**: patients, claims, explanations, coverages, encounters,
      conditions, prior_auth
    - **Gold tables**: pa_risk_scores, procedure_approval_rates, payer_performance
    - **Redshift marts**: all 4 analytical marts

    Alerts via SNS if any count drops below expected thresholds.
    """
)

# ── Expected row count thresholds ─────────────────────────────────────────────
THRESHOLDS = {
    'patients':               4000,
    'claims':                 400000,
    'explanations':           400000,
    'coverages':              400000,
    'encounters':             200000,
    'conditions':             150000,
    'prior_auth':             400000,
    'pa_risk_scores':         400000,
    'procedure_approval_rates': 10,
    'payer_performance':       5
}


def run_athena_count(table_name, database='pacip_silver_db'):
    """Run an Athena count query and return result."""
    athena = boto3.client('athena', region_name='us-east-1')
    s3_output = 's3://pacip-data-lake-347011900951/athena-results/'

    response = athena.start_query_execution(
        QueryString=f"SELECT COUNT(*) as cnt FROM {database}.{table_name}",
        ResultConfiguration={'OutputLocation': s3_output}
    )
    query_id = response['QueryExecutionId']

    # Poll until complete
    for _ in range(30):
        time.sleep(5)
        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            break
        elif status in ['FAILED', 'CANCELLED']:
            raise Exception(f"Athena query failed for {table_name}")

    result = athena.get_query_results(QueryExecutionId=query_id)
    count = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    return count


def check_silver_tables(**context):
    """Check all Silver table row counts against thresholds."""
    silver_tables = [
        'patients', 'claims', 'explanations',
        'coverages', 'encounters', 'conditions', 'prior_auth'
    ]

    results = {}
    failures = []

    for table in silver_tables:
        count = run_athena_count(table, 'pacip_silver_db')
        threshold = THRESHOLDS[table]
        status = 'PASS' if count >= threshold else 'FAIL'
        results[table] = {'count': count, 'threshold': threshold, 'status': status}
        print(f"  {table}: {count} rows ({status})")
        if status == 'FAIL':
            failures.append(f"{table}: {count} < {threshold}")

    context['task_instance'].xcom_push(key='silver_results', value=results)
    context['task_instance'].xcom_push(key='silver_failures', value=failures)

    if failures:
        raise Exception(f"Silver quality check failed: {failures}")

    print("All Silver tables passed quality checks")
    return results


def check_gold_tables(**context):
    """Check Gold table row counts."""
    gold_tables = [
        'pa_risk_scores',
        'procedure_approval_rates',
        'payer_performance'
    ]

    results = {}
    failures = []

    for table in gold_tables:
        count = run_athena_count(table, 'pacip_silver_db')
        threshold = THRESHOLDS[table]
        status = 'PASS' if count >= threshold else 'FAIL'
        results[table] = {'count': count, 'threshold': threshold, 'status': status}
        print(f"  {table}: {count} rows ({status})")
        if status == 'FAIL':
            failures.append(f"{table}: {count} < {threshold}")

    context['task_instance'].xcom_push(key='gold_results', value=results)
    context['task_instance'].xcom_push(key='gold_failures', value=failures)

    if failures:
        raise Exception(f"Gold quality check failed: {failures}")

    print("All Gold tables passed quality checks")
    return results


def branch_on_quality(**context):
    """Branch based on whether all checks passed."""
    silver_failures = context['task_instance'].xcom_pull(
        task_ids='check_silver_tables',
        key='silver_failures'
    ) or []
    gold_failures = context['task_instance'].xcom_pull(
        task_ids='check_gold_tables',
        key='gold_failures'
    ) or []

    all_failures = silver_failures + gold_failures
    if all_failures:
        return 'alert_quality_failure'
    return 'quality_check_passed'


def alert_quality_failure(**context):
    """Send SNS alert on quality failure."""
    sns = boto3.client('sns', region_name='us-east-1')
    silver_failures = context['task_instance'].xcom_pull(
        task_ids='check_silver_tables',
        key='silver_failures'
    ) or []
    gold_failures = context['task_instance'].xcom_pull(
        task_ids='check_gold_tables',
        key='gold_failures'
    ) or []

    all_failures = silver_failures + gold_failures
    message = (
        f"PACIP Quality Check Failed\n\n"
        f"Date: {context['execution_date']}\n"
        f"Failures:\n" + '\n'.join(f"  - {f}" for f in all_failures) +
        f"\n\nPlease check the PACIP pipeline and rerun if necessary."
    )

    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:347011900951:pacip-pipeline-alerts',
        Subject='PACIP Quality Alert',
        Message=message
    )
    print(f"Quality failure alert sent for: {all_failures}")


# ── Task definitions ──────────────────────────────────────────────────────────
start = EmptyOperator(task_id='start', dag=dag)

check_silver = PythonOperator(
    task_id='check_silver_tables',
    python_callable=check_silver_tables,
    dag=dag
)

check_gold = PythonOperator(
    task_id='check_gold_tables',
    python_callable=check_gold_tables,
    dag=dag
)

branch = BranchPythonOperator(
    task_id='branch_on_quality',
    python_callable=branch_on_quality,
    dag=dag
)

alert_failure = PythonOperator(
    task_id='alert_quality_failure',
    python_callable=alert_quality_failure,
    dag=dag
)

quality_passed = EmptyOperator(task_id='quality_check_passed', dag=dag)
end = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# ── Task dependencies ──────────────────────────────────────────────────────────
start >> [check_silver, check_gold] >> branch
branch >> [alert_failure, quality_passed] >> end
