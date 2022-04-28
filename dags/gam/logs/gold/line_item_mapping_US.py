from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from functools import partial
from tardis_utils.tardis_sensor.tardis_sensor import Tardis_Sensor
from plugins.utilities.clusters.gam import get_gam_log_cluster, get_gam_logs_cluster_libs
from plugins.config import GamAPIConfig, GAMLogConfig, AppConfig
from dags.gam.logs.functions.utils import gam_gold_alerts, send_slack_alert

ENV = AppConfig.environment
DB_NOTEBOOK_PATH = GamAPIConfig.line_item_mapping_notebook
cluster = get_gam_log_cluster('', ENV)
TARDIS_SOURCE = "gam.line_item_custom_targeting_3379"

default_args = {
    'owner': GAMLogConfig.GAM_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

cluster["node_type_id"] = "r5.large"
databricks_job_cluster = {
    'libraries': get_gam_logs_cluster_libs(),
    'notebook_task': {
        'notebook_path': DB_NOTEBOOK_PATH,
        'base_parameters': {
            'env': ENV,
            'log_date': "{{ds}}",
            'network_code': "3379"
        }
    },
    'new_cluster': cluster
}

with DAG('gam_line_item_mapping_dag',
         max_active_runs=10,
         default_args=default_args,
         catchup=False,
         schedule_interval='30 11 * * *',
         on_success_callback=partial(send_slack_alert,
                                     'GAM LineItem Dag success',
                                     'Success',
                                     GAMLogConfig.slack_de_token,
                                     GAMLogConfig.alert_channel,
                                     None,
                                     None
                                     )) as dag:
    line_item_mapping_sensor = Tardis_Sensor(task_id="line_item_mapping_Tardis_check",
                                             poke_interval=5,
                                             sources=['gam.line_items_3379', 'gam.custom_targeting_3379'],
                                             start_logdate="{{ds}}",
                                             status='Complete',
                                             xcom_push=True,
                                             on_success_callback=partial(gam_gold_alerts, source=TARDIS_SOURCE,
                                                                         status="Data Received",
                                                                         comments="GAM Global Silver LineItem Data ready to load!! "),
                                             on_failure_callback=partial(gam_gold_alerts, source=TARDIS_SOURCE,
                                                                         status="Data Not Received",
                                                                         comments="GAM Global Gold LineItem Tardis Sensor Failed!! ",
                                                                         slack_source_name="GAM Global Gold LineItem Tardis Sensor Failed"))

    line_item_mapping_databricks_run = DatabricksSubmitRunOperator(
        databricks_conn_id='databricks_default',
        task_id='line_item_mapping_databricks_job',
        json=databricks_job_cluster,
        polling_period_seconds=100,
        retries=1,
        on_success_callback=partial(gam_gold_alerts, source=TARDIS_SOURCE, status="Data Complete",
                                    comments="GAM Global Gold LineItem completed!! "),
        on_failure_callback=partial(gam_gold_alerts, source=TARDIS_SOURCE, status="Data Validation Failed",
                                    comments="GAM Global Gold Databricks Job Failed!! ",
                                    slack_source_name="GAM Global Gold LineItem Databricks Job Failed"))

    line_item_mapping_sensor >> line_item_mapping_databricks_run
