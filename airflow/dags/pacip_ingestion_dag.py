"""
PACIP Ingestion DAG
Triggers the full PACIP pipeline via Step Functions on a weekly schedule.
In production: S3 sensor detects new CMS data release → triggers pipeline.
For demo: manual trigger available.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import boto3
import json
import time

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    'owner': 'pacip-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# ── DAG definition ────────────────────────────────────────────────────────────
dag = DAG(
    'pacip_ingestion_pipeline',
    default_args=default_args,
    description='PACIP weekly ingestion pipeline via Step Functions',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
    tags=['pacip', 'ingestion', 'healthcare', 'fhir'],
    doc_md="""
    ## PACIP Ingestion Pipeline

    Orchestrates the full Prior Authorization & Claims Intelligence Platform pipeline:

    1. **Check S3** for new FHIR data in Bronze zone
    2. **Trigger Step Functions** state machine (Bronze crawl → Glue ETL → EMR scoring → Silver crawl)
    3. **Monitor execution** until completion
    4. **Notify** via SNS on success or failure

    **CMS-0057-F Compliance:** This pipeline processes prior authorization data
    in alignment with CMS mandated reporting requirements.
    """
)

# ── Task functions ────────────────────────────────────────────────────────────
def check_s3_for_new_data(**context):
    """Check if Bronze zone has FHIR data ready for processing."""
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket = 'pacip-data-lake-347011900951'
    prefix = 'bronze/fhir/synthea/'

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )

    if response.get('KeyCount', 0) == 0:
        raise ValueError(f"No FHIR data found in s3://{bucket}/{prefix}")

    count = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['KeyCount']
    print(f"Found {count} FHIR files in Bronze zone — ready for processing")
    context['task_instance'].xcom_push(key='fhir_file_count', value=count)
    return count


def trigger_step_functions(**context):
    """Start the PACIP Step Functions state machine execution."""
    sfn = boto3.client('stepfunctions', region_name='us-east-1')

    execution_name = f"pacip-airflow-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    response = sfn.start_execution(
        stateMachineArn='arn:aws:states:us-east-1:347011900951:stateMachine:pacip-pipeline',
        name=execution_name,
        input=json.dumps({
            'triggered_by': 'airflow',
            'dag_run_id': context['run_id'],
            'execution_date': str(context['execution_date'])
        })
    )

    execution_arn = response['executionArn']
    print(f"Step Functions execution started: {execution_arn}")
    context['task_instance'].xcom_push(
        key='execution_arn',
        value=execution_arn
    )
    return execution_arn


def wait_for_step_functions(**context):
    """Poll Step Functions until execution completes."""
    sfn = boto3.client('stepfunctions', region_name='us-east-1')
    execution_arn = context['task_instance'].xcom_pull(
        task_ids='trigger_step_functions',
        key='execution_arn'
    )

    print(f"Monitoring execution: {execution_arn}")
    max_wait_seconds = 3600  # 1 hour timeout
    poll_interval = 60
    elapsed = 0

    while elapsed < max_wait_seconds:
        response = sfn.describe_execution(executionArn=execution_arn)
        status = response['status']
        print(f"Elapsed: {elapsed}s | Status: {status}")

        if status == 'SUCCEEDED':
            print("Pipeline completed successfully")
            return status
        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            raise Exception(f"Step Functions execution {status}: {execution_arn}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise Exception(f"Pipeline timed out after {max_wait_seconds}s")


def notify_success(**context):
    """Send SNS notification on pipeline success."""
    sns = boto3.client('sns', region_name='us-east-1')
    fhir_count = context['task_instance'].xcom_pull(
        task_ids='check_s3_data',
        key='fhir_file_count'
    )

    message = (
        f"PACIP Pipeline Completed Successfully\n\n"
        f"DAG Run ID   : {context['run_id']}\n"
        f"Execution Date: {context['execution_date']}\n"
        f"FHIR Files Processed: {fhir_count}\n\n"
        f"All Silver and Gold tables have been refreshed.\n"
        f"Redshift marts are up to date."
    )

    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:347011900951:pacip-pipeline-alerts',
        Subject='PACIP Pipeline Success',
        Message=message
    )
    print("Success notification sent via SNS")


# ── Task definitions ──────────────────────────────────────────────────────────
start = EmptyOperator(task_id='start', dag=dag)

check_s3 = PythonOperator(
    task_id='check_s3_data',
    python_callable=check_s3_for_new_data,
    dag=dag
)

trigger_sfn = PythonOperator(
    task_id='trigger_step_functions',
    python_callable=trigger_step_functions,
    dag=dag
)

wait_sfn = PythonOperator(
    task_id='wait_for_pipeline',
    python_callable=wait_for_step_functions,
    dag=dag
)

notify = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

# ── Task dependencies ──────────────────────────────────────────────────────────
start >> check_s3 >> trigger_sfn >> wait_sfn >> notify >> end
