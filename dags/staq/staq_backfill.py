from airflow import DAG
from marshmellow.airflow.databricks import CondeDatabricksSubmitRunOperator

from plugins.config import AppConfig, StaqConfig
from plugins.utilities.slack_service import success_alert, failure_alert
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.clusters.staq import get_staq_backfill_cluster, get_staq_cluster_lib

# ======================================
# CONFIGURATION
# ======================================

notebook_task_params = {
    'notebook_task': {
        'notebook_path': StaqConfig.staq_backfill_notebook,
        'base_parameters': {
            'env': AppConfig.environment,
            'file_start_date': "{{ dag_run.conf['start_date'] }}",
            'file_end_date': "{{ dag_run.conf['end_date'] }}",
            'partner': "{{ dag_run.conf['partner'] }}",
            'do_create_db_table': "{{ dag_run.conf['create_db_table'] }}"
        }
    },
    'new_cluster': get_staq_backfill_cluster(AppConfig.environment),
    'libraries': get_staq_cluster_lib()
}

# ======================================
# DAG DECLARATION
# ======================================

dag = DAG(
    dag_id='STAQ_BACKFILL',
    default_args=get_default_args("us_ssekar2"),
    max_active_runs=1,
    schedule_interval=None,
    catchup=False
)

data_backfill_notebook = CondeDatabricksSubmitRunOperator(
    task_id='data_backfill_notebook',
    dag=dag,
    json=notebook_task_params,
    depends_on_past=False,
    on_failure_callback=failure_alert,
    on_success_callback=success_alert
)

data_backfill_notebook
