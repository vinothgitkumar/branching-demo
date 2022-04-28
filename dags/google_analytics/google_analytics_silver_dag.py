from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from dags.google_analytics.functions import base, silver
from plugins.config import GoogleAnalyticsGlobal, AppConfig
from plugins.utilities.airflow_connections import create_databricks_connection
from plugins.utilities.clusters.google_analytics import get_silver_cluster_config

# ======================================
# CONFIGURATION
# ======================================
DAG_NAME = 'ga_global_silver_dag'
env = AppConfig.environment

SLACK_ALERT_CHANNEL = GoogleAnalyticsGlobal.alert_channel
SLACK_TOKEN = GoogleAnalyticsGlobal.slack_de_token
# ======================================
# DAG DECLARATION
# ======================================
default_args = {
    'owner': 'us_mpiruka',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}
dag = DAG(DAG_NAME,
          max_active_runs=5,
          default_args=default_args,
          catchup=False,
          start_date=datetime(2021, 1, 1),
          schedule_interval=None,
          tags=['google_analytics', 'silver'],
          on_success_callback=partial(base.send_slack_alert,
                                      'Silver Dag successfully ran',
                                      'Success',
                                      SLACK_TOKEN,
                                      SLACK_ALERT_CHANNEL,
                                      None,
                                      None
                                      ),
          )

# ======================================
# TASKS
# ======================================
notebook_params = {
    'dataset': "{{ dag_run.conf['dag_dataset'] }}",
    'env': env
}

databricks_cluster_ga, databricks_ga_cluster_libs = get_silver_cluster_config(env)

notebook_task_params = {'libraries': databricks_ga_cluster_libs,
                        'notebook_task': {
                            'notebook_path': GoogleAnalyticsGlobal.notebook_silver,
                            'base_parameters': notebook_params
                        },
                        'new_cluster': databricks_cluster_ga
                        }

# Run Notebook Using Databricks API
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

silver_data = DatabricksSubmitRunOperator(
    task_id="silver_data",
    json=notebook_task_params,
    polling_period_seconds=60,
    on_success_callback=silver.check_update_tardis_complete,
    on_failure_callback=partial(base.task_failure,
                                "GA_PROJECT Process Failed at Silver Data load",
                                None,
                                "Silver Databricks Job"
                                ),
    dag=dag
)

post_silver_data = BranchPythonOperator(
    task_id="post_silver_data_load",
    python_callable=silver.check_post_silver_data_loads,
    op_kwargs={
        'dag_dataset': "{{ dag_run.conf['dag_dataset'] }}",
    },
    dag=dag
)

load_stats = DatabricksSubmitRunOperator(
    task_id="load_stats",
    trigger_rule='none_failed',
    json={'libraries': databricks_ga_cluster_libs,
          'notebook_task': {
              'notebook_path': GoogleAnalyticsGlobal.load_stats,
              'base_parameters': notebook_params
          },
          'new_cluster': databricks_cluster_ga
          },
    polling_period_seconds=10,
    dag=dag
)

brand_mapping = DatabricksSubmitRunOperator(
    task_id="brand_mapping",
    json={'libraries': databricks_ga_cluster_libs,
          'notebook_task': {
              'notebook_path': GoogleAnalyticsGlobal.brand_mapping_notebook,
              'base_parameters': notebook_params
          },
          'new_cluster': databricks_cluster_ga
          },
    polling_period_seconds=10,
    dag=dag
)
end = DummyOperator(task_id='end',
                    trigger_rule='none_failed',
                    dag=dag)

create_conn >> silver_data >> post_silver_data >> brand_mapping >> load_stats >> end
post_silver_data >> end
