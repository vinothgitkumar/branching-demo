from airflow import DAG
from functools import partial
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from plugins.config import GAMLogConfig, AppConfig
from plugins.utilities.clusters.gam import get_gam_log_cluster, get_gam_logs_cluster_libs
from dags.gam.logs.functions.utils import alert


ENV = AppConfig.environment
cluster = get_gam_log_cluster(GAMLogConfig.GAM_CREDS_PATH, ENV)
cluster["node_type_id"] = "{{ dag_run.conf['instance'] }}"
network_code = "{{dag_run.conf['network_code']}}"
report_name = "{{dag_run.conf['report_name']}}"

job_params = {
    'libraries': get_gam_logs_cluster_libs(),
    'notebook_task': {
        'notebook_path': GAMLogConfig.logs_silver_notebook,
        'base_parameters': {
            'env': ENV,
            'log_type': report_name,
            'log_date': "{{dag_run.conf['log_date']}}",
            'network_id': "{{dag_run.conf['network_id']}}",
            'network_code': network_code

        }
    },
    'new_cluster': cluster
}


default_args = {
    'owner': GAMLogConfig.GAM_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG('gam_logs_global_silver',
          max_active_runs=30,
          default_args=default_args,
          schedule_interval=None)


submit_databricks_run = DatabricksSubmitRunOperator(
    databricks_conn_id='databricks_default',
    task_id="submit_silver_databricks_run",
    run_name="{{ run_id }}",
    json=job_params,
    polling_period_seconds=100,
    retries=1,
    dag=dag,
    on_failure_callback=partial(alert, status="Data Validation Failed",
                                comment="Data Not Available in Silver Delta Tables!! ",
                                slack_source_name="GAM Global Silver Databricks Job Failed")
)

submit_databricks_run
