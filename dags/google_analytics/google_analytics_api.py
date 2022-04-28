from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from plugins.config import GoogleAnalyticsGlobal, AppConfig
from plugins.utilities.clusters.google_analytics import get_management_api_cluster_config
from plugins.utilities.tardis_utils import update_tardis
from dags.google_analytics.functions import base
from plugins.utilities.airflow_connections import create_databricks_connection

# ======================================
# CONFIGURATION
# ======================================
DAG_NAME = 'ga_management_api_loads'

PROJECT_ID = GoogleAnalyticsGlobal.project_info_project_id
ENV = AppConfig.environment
BRONZE_DB_NOTEBOOK_PATH = GoogleAnalyticsGlobal.mgmt_api_notebook_bronze
SILVER_DB_NOTEBOOK_PATH = GoogleAnalyticsGlobal.mgmt_api_notebook_silver

SLACK_ALERT_CHANNEL = GoogleAnalyticsGlobal.alert_channel
SLACK_TOKEN = GoogleAnalyticsGlobal.slack_de_token

databricks_cluster, dependent_libs = get_management_api_cluster_config(ENV)

# ======================================
# DAG DECLARATION
# ======================================

default_args = {
    'owner': 'us_mpiruka',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

tardis_source = GoogleAnalyticsGlobal.mgmt_api_tardis_process_source

dag = DAG(DAG_NAME,
          max_active_runs=2,
          schedule_interval="0 0 * * *",
          default_args=default_args,
          tags=['google_analytics', 'bronze', 'silver', 'api'],
          catchup=False,
          )

# ======================================
# TASKS
# ======================================


# Run Notebook Using Databricks API

log_start = PythonOperator(task_id="log_start",
                           python_callable=update_tardis,
                           op_kwargs={
                               "tardis_source": tardis_source,
                               "post_type": ['Process'],
                               "logdate": "{{ds}}",
                               "comments": "Load Started",
                               "status": "Started",
                           },
                           on_failure_callback=partial(base.task_failure,
                                                       "GA Management API Process Failed at Updating Tardis",
                                                       GoogleAnalyticsGlobal,
                                                       "Log Start"
                                                       ),
                           dag=dag
                           )

create_conn = PythonOperator(
    task_id="create_conn",
    python_callable=create_databricks_connection,
    on_failure_callback=partial(base.task_failure,
                                "GA_PROJECT Process Failed at Silver Data load",
                                None,
                                "Job Failed while creating Airflow connection"
                                ),
    dag=dag
)

load_bronze_dimensions = DatabricksSubmitRunOperator(task_id="load_bronze_dimensions",
                                                     json={
                                                         'libraries': dependent_libs,
                                                         'notebook_task': {
                                                             'notebook_path': BRONZE_DB_NOTEBOOK_PATH,
                                                             'base_parameters': {
                                                                 'logdate': '{{ds}}',
                                                                 'project_id': PROJECT_ID,
                                                                 'custom_type': "dimensions",
                                                                 'env': ENV,
                                                             }
                                                         },
                                                         'new_cluster': databricks_cluster
                                                     },
                                                     on_failure_callback=partial(base.task_failure,
                                                                                 "GA Management API Process Failed at Load Dimensions Load",
                                                                                 GoogleAnalyticsGlobal,
                                                                                 "Load Bronze Dimensions"
                                                                                 ),
                                                     dag=dag,

                                                     )

load_bronze_metrics = DatabricksSubmitRunOperator(task_id="load_bronze_metrics",
                                                  json={
                                                      'libraries': dependent_libs,
                                                      'notebook_task': {
                                                          'notebook_path': BRONZE_DB_NOTEBOOK_PATH,
                                                          'base_parameters': {
                                                              'logdate': '{{ds}}',
                                                              'project_id': PROJECT_ID,
                                                              'custom_type': "metrics",
                                                              'env': ENV
                                                          }
                                                      },
                                                      'new_cluster': databricks_cluster
                                                  },
                                                  # on_failure_callback=partial(mark_fail),
                                                  on_failure_callback=partial(base.task_failure,
                                                                              "GA Management API Process Failed at Load Metrics Load",
                                                                              GoogleAnalyticsGlobal,
                                                                              "Load Bronze Metrics"
                                                                              ),

                                                  dag=dag,

                                                  )

load_silver_dimensions = DatabricksSubmitRunOperator(task_id="load_silver_dimensions",
                                                     json={
                                                         'notebook_task': {
                                                             'notebook_path': SILVER_DB_NOTEBOOK_PATH,
                                                             'base_parameters': {
                                                                 'logdate': '{{ds}}',
                                                                 'project_id': PROJECT_ID,
                                                                 'custom_type': "dimensions",
                                                                 'env': ENV
                                                             }
                                                         },
                                                         'new_cluster': databricks_cluster
                                                     },
                                                     on_failure_callback=partial(base.task_failure,
                                                                                 "GA Management API Process Failed at Load Dimensions Load",
                                                                                 GoogleAnalyticsGlobal,
                                                                                 "Load Silver Dimensions"
                                                                                 ),
                                                     dag=dag,

                                                     )
load_silver_metrics = DatabricksSubmitRunOperator(task_id="load_silver_metrics",
                                                  json={
                                                      'notebook_task': {
                                                          'notebook_path': SILVER_DB_NOTEBOOK_PATH,
                                                          'base_parameters': {
                                                              'logdate': '{{ds}}',
                                                              'project_id': PROJECT_ID,
                                                              'custom_type': "metrics",
                                                              'env': ENV
                                                          }
                                                      },
                                                      'new_cluster': databricks_cluster
                                                  },
                                                  # on_failure_callback=partial(mark_fail),
                                                  on_failure_callback=partial(base.task_failure,
                                                                              "GA Management API Process Failed at Load Dimensions Load",
                                                                              GoogleAnalyticsGlobal,
                                                                              "Load Silver Metrics"
                                                                              ),

                                                  dag=dag,

                                                  )

log_end_success = PythonOperator(task_id="log_end_success",
                                 python_callable=update_tardis,
                                 trigger_rule='none_failed',
                                 op_kwargs={
                                     "tardis_source": tardis_source,
                                     "post_type": ['Process'],
                                     "logdate": "{{ds}}",
                                     "status": "Complete",
                                     "comments": "Load Complete"
                                 },
                                 # on_failure_callback=partial(mark_fail),
                                 on_failure_callback=partial(base.task_failure,
                                                             "GA Management API Process Failed at Updating Tardis Success",
                                                             GoogleAnalyticsGlobal,
                                                             "Log End Success"
                                                             ),

                                 on_success_callback=partial(base.send_slack_alert,
                                                             'GA Management API Dag success',
                                                             'Success',
                                                             SLACK_TOKEN,
                                                             SLACK_ALERT_CHANNEL,
                                                             None,
                                                             None
                                                             ),
                                 dag=dag
                                 )

log_end_failed = PythonOperator(task_id="log_end_failed",
                                python_callable=update_tardis,
                                trigger_rule='one_failed',
                                op_kwargs={
                                    "tardis_source": tardis_source,
                                    "post_type": ['Process'],
                                    "logdate": "{{ds}}",
                                    "status": "Failed",
                                    "comments": "Load Failed"
                                },
                                on_failure_callback=partial(base.task_failure,
                                                            "GA Management API Process Failed at Updating Tardis Failed",
                                                            GoogleAnalyticsGlobal,
                                                            "Log End Failed"
                                                            ),

                                dag=dag
                                )

log_start >> create_conn
create_conn >> load_bronze_dimensions >> load_silver_dimensions >> [log_end_success, log_end_failed]
create_conn >> load_bronze_metrics >> load_silver_metrics >> [log_end_success, log_end_failed]
