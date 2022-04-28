from airflow import DAG
from functools import partial
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators import TriggerDagRunOperator
from datetime import datetime, timedelta
from plugins.config import GAMLogConfig, AppConfig
from plugins.utilities.clusters.gam import get_gam_log_cluster, get_gam_logs_cluster_libs
from dags.gam.logs.functions.utils import alert, set_dag_conf

ENV = AppConfig.environment
DB_NOTEBOOK_PATH = GAMLogConfig.logs_bronze_notebook
cluster = get_gam_log_cluster(GAMLogConfig.GAM_CREDS_PATH, ENV)
DAG_OWNER = GAMLogConfig.GAM_GLOBAL_PROCESS_OWNER
cluster["node_type_id"] = "{{ dag_run.conf['instance'] }}"
network_code = "{{dag_run.conf['network_code']}}"
report_name = "{{dag_run.conf['report_name']}}"

job_params = {
    'libraries': get_gam_logs_cluster_libs(),
    'notebook_task': {
        'notebook_path': DB_NOTEBOOK_PATH,
        'base_parameters': {
            'files': "{{ dag_run.conf['files'] }}",
            'env': ENV,
            'log_type': report_name,
            'log_date': "{{dag_run.conf['log_date']}}",
            'network_id': "{{dag_run.conf['network_id']}}",
            'network_code': network_code,
            'read_bkp_bucket': "{{dag_run.conf['read_bkp_bucket']}}",
            'bronze_notebook': "{{dag_run.conf['bronze_notebook']}}"
        }
    },
    'new_cluster': cluster
}

default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG('gam_logs_global_bronze',
          max_active_runs=30,
          default_args=default_args,
          schedule_interval=None)

submit_databricks_run = DatabricksSubmitRunOperator(
    databricks_conn_id='databricks_default',
    task_id="submit_bronze_databricks_run",
    run_name="{{ run_id }}",
    json=job_params,
    polling_period_seconds=100,
    retries=1,
    dag=dag,
    on_success_callback=partial(alert, status="Data Staged", comment="Data Available in Bronze Delta Tables!! "),
    on_failure_callback=partial(alert, status="Data Validation Failed",
                                comment="Data Not Available in Bronze Delta Tables!! ",
                                slack_source_name="GAM Global Bronze Databricks Job Failed")
)

trigger_silver_build = TriggerDagRunOperator(
    task_id='trigger_silver_databricks_run',
    trigger_dag_id="gam_logs_global_silver",
    python_callable=set_dag_conf,
    provide_context=True,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=partial(alert, status="Data Validation Failed",
                                comment="Failed to trigger Silver databricks run!! ",
                                slack_source_name="GAM Global Trigger Silver Databricks Failed")
)

submit_databricks_run >> trigger_silver_build
