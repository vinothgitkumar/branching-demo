from airflow import DAG
from datetime import datetime
from marshmellow.airflow.databricks import CondeDatabricksSubmitRunOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

from plugins.config import AppConfig, StaqConfig
from plugins.utilities.airflow_connections import create_airflow_connection
from plugins.utilities.vault import vault_instance
from plugins.utilities.clusters.staq import get_staq_copy_cluster

# ======================================
# CONFIGURATION
# ======================================

s3_key = vault_instance.get_secret(StaqConfig.aws_s3_access_key_name)
s3_secret = vault_instance.get_secret(StaqConfig.aws_s3_secret_key_name)


current_day = datetime.today().strftime("%Y-%m-%d")
source_bucket = StaqConfig.src_bucket
target_bucket = StaqConfig.tgt_bucket
vendor_config = StaqConfig.vendor_conf


create_airflow_connection("staq_s3_connection_for_s3_sensors", "s3", "", s3_key, s3_secret, None, None, "", False)

# ======================================
# DAG DECLARATION
# ======================================

dag = DAG(
    dag_id='STAQ_RAW_FILES_BACKUP',
    default_args={'owner': 'us_ssekar2',
                  'depends_on_past': False,
                  'email_on_failure': False,
                  'email_on_retry': False
                  },
    max_active_runs=1,
    schedule_interval='@hourly',
    catchup=False,
    start_date=datetime(2022, 3, 15, 0, 0)
)

# Iterate Vendors to add sensors to each and copy files

for vendor in vendor_config:
    task_name = f'{vendor}_file_sensor'.replace("/", "_")
    vendor_file_name = vendor_config[vendor]
    key = f'staq/inbound/{vendor}/{vendor_file_name}_{current_day}*'
    vendor_file_s3_sensor = S3KeySensor(
            task_id=task_name,
            poke_interval=60,
            bucket_key=key,
            wildcard_match=True,
            bucket_name=source_bucket,
            aws_conn_id='staq_s3_connection_for_s3_sensors',
            timeout=3600,
            retries=0,
            dag=dag
        )

    notebook_task_params = {
        'notebook_task': {
            'notebook_path': StaqConfig.staq_raw_files_copy_notebook,
            'base_parameters': {
                'env': AppConfig.environment,
                'source_bucket': source_bucket,
                'target_bucket': target_bucket,
                'source_path': f'staq/inbound/{vendor}/',
                'target_path': f'inbound/{vendor}/',
                'run_date': current_day,
                'vendor': vendor
            }
        },
        'new_cluster': get_staq_copy_cluster(AppConfig.environment)
    }

    copy_task = CondeDatabricksSubmitRunOperator(
        task_id=f'{vendor}_copy_task'.replace("/", "_"),
        dag=dag,
        json=notebook_task_params,
        depends_on_past=False
    )
    vendor_file_s3_sensor >> copy_task
