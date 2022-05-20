from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from plugins.config import CneVideosConfig, AppConfig
from plugins.utilities.databricks.DatabricksUtils import create_databricks_connection
from plugins.utilities.clusters.cne_videos import get_cne_videos_configuration, get_cne_videos_lib
from datetime import datetime, timedelta
from plugins.utilities.slack_service import success_alert, failure_alert
#DAG for cne

default_args = {
    'owner': 'us_achoudha',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

notebook_task_params_us_report = {
    'notebook_task': {
        'notebook_path': CneVideosConfig.cne_videos_notebook,
        'base_parameters': {
            'report_type': 'videos',
            'load_date': '{{ ds }}',
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_cne_videos_configuration(instance_profile=CneVideosConfig.instance_profile),
    'libraries': get_cne_videos_lib(),
}

notebook_task_params_int_report = {
    'notebook_task': {
        'notebook_path': CneVideosConfig.cne_videos_notebook,
        'base_parameters': {
            'report_type': 'videos_all',
            'load_date': '{{ ds }}',
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_cne_videos_configuration(instance_profile=CneVideosConfig.instance_profile),
    'libraries': get_cne_videos_lib(),
}

with DAG(
        dag_id='cne_video_reports',
        default_args=default_args,
        description='Daily Ingestion of CNE video reports for US and International markets.',
        schedule_interval='30 8 * * *',
        max_active_runs=1,
        catchup=False,
        tags=['CNE'],
        on_success_callback=success_alert
        ) as dag:
    create_conn = PythonOperator(
        task_id='SetConnections',
        dag=dag,
        python_callable=create_databricks_connection,
    )

    US_report_processing = DatabricksSubmitRunOperator(
        task_id='US_report_processing',
        dag=dag,
        json=notebook_task_params_us_report,
        on_failure_callback=failure_alert
    )

    Global_report_processing = DatabricksSubmitRunOperator(
        task_id='Global_report_processing',
        dag=dag,
        json=notebook_task_params_int_report,
        on_failure_callback=failure_alert
    )

    create_conn >> US_report_processing
    create_conn >> Global_report_processing
#testing4