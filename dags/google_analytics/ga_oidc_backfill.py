import json

from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from dags.google_analytics.functions.backfill import check_func, trigger_dags, orchestrate
from plugins.config import AppConfig
from plugins.utilities.clusters.google_analytics import get_bronze_cluster_config, get_silver_cluster_config
from plugins.utilities.tardis_utils import update_tardis
from plugins.utilities.airflow_connections import create_databricks_connection
from plugins.config import GoogleAnalyticsOIDC

# ======================================
# EXAMPLES for Passing the Parameters
# ======================================
# Project id --> dataset_id mapping: [Reference -> plugins.google_analytics_global.config file]
# "140208876" --> GA Global project ["cni-ca-dfp"]
# "229339819" --> GA OIDC project ["titanium-cacao-204116"]
#
# ======================================
# The below examples shall be passed as config for GA OIDC while triggering this DAG
# [NOTE: For GA Global just change the dataset_id as mapped above]
# Sample for GA OIDC:
# ======================================
# {"logdates":["2020-04-02"]}
# {"logdates":["2020-01-02"],"bronze":"true"}
# {"logdates":["2021-02-21","2021-02-19"],"silver":"true"}
# {"logdates":["2020-03-02"],"bronze":"true", "silver":"true"}
# {"logdates":["2020-05-02", "2020-06-02"], "silver": "true"}


# ======================================
# CONFIGURATION
# ======================================


DAG_NAME = 'ga_oidc_backfill'
env = AppConfig.environment

# ======================================
# DAG DECLARATION
# ======================================

default_args = {
    'owner': 'us_mpiruka',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,

}

dag = DAG(dag_id=DAG_NAME,
          max_active_runs=10,
          schedule_interval=None,
          default_args=default_args,
          catchup=False,
          )

# ======================================
# TASKS
# ======================================


databricks_cluster_brnz, databricks_cluster_libs = get_bronze_cluster_config(GoogleAnalyticsOIDC.gcp_bq_cred, env)

databricks_cluster_silver, databricks_ga_cluster_libs_silver = get_silver_cluster_config(env)

notebook_params = {
    'logdates': "{{ dag_run.conf['logdates'] }}",
    'env': env,
    'tardis_source': GoogleAnalyticsOIDC.tardis_data_source,
    'dataset_id': GoogleAnalyticsOIDC.project_info_project_id
}

notebook_task_params = {'libraries': databricks_cluster_libs,
                        'notebook_task': {
                            'notebook_path': GoogleAnalyticsOIDC.notebook_backfill,
                            'base_parameters': notebook_params
                        },
                        'new_cluster': databricks_cluster_brnz
                        }


def load_subdag(parent_dag_name, child_dag_name, args, log_date, nb_path, dataset_id, databricks_cluster, **context):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.load_{child_dag_name}_tasks',
        default_args=args,
        schedule_interval='* * * * *',
    )

    with dag_subdag:
        check_task = BranchPythonOperator(
            task_id=f"check_{child_dag_name}",
            python_callable=check_func,
            op_kwargs={"layer": child_dag_name,
                       'logdate': log_date},
            trigger_rule="all_done",
            provide_context=True,
            dag=dag_subdag
        )
        create_conn_db = PythonOperator(
            task_id="create_conn_db",
            python_callable=create_databricks_connection,
            dag=dag_subdag
        )
        databricks_task = DatabricksSubmitRunOperator(
            task_id=f"{child_dag_name}_task",
            json={
                'libraries': databricks_cluster_libs,
                'notebook_task': {
                    'notebook_path': nb_path,
                    'base_parameters': {
                        'dataset': json.dumps({
                            "log_date": log_date,
                            'tardis_source': GoogleAnalyticsOIDC.tardis_data_source,
                            "dataset_id": GoogleAnalyticsOIDC.project_info_project_id
                        }),
                        'env': env,
                    }
                },
                'new_cluster': databricks_cluster
            }
        )

        end_task = DummyOperator(
            task_id="end_task",
            trigger_rule="all_done",
            dag=dag_subdag,
        )

        check_task >> create_conn_db >> databricks_task >> end_task
        check_task >> end_task

    return dag_subdag


orchestration_task = BranchPythonOperator(
    task_id="orchestration_task",
    python_callable=orchestrate,
    provide_context=True,
    dag=dag
)

create_conn_db = PythonOperator(
    task_id="create_conn_db",
    python_callable=create_databricks_connection,
    trigger_rule="all_done",
    dag=dag
)

trigger_dag_task = PythonOperator(
    task_id="trigger_dag_task",
    python_callable=trigger_dags,
    op_kwargs={'DAG_NAME': DAG_NAME},
    provide_context=True,
    dag=dag
)

count_update_tardis = DatabricksSubmitRunOperator(
    task_id="count_update_tardis",
    json=notebook_task_params,
    task_concurrency=1,
    dag=dag,
)

load_bronze_tasks = SubDagOperator(
    task_id="load_bronze_tasks",
    subdag=load_subdag(parent_dag_name=DAG_NAME,
                       child_dag_name="bronze",
                       args=default_args,
                       log_date="{{ dag_run.conf['logdate'] }}",
                       nb_path=GoogleAnalyticsOIDC.notebook_bronze,
                       dataset_id="{{ dag_run.conf['dataset_id'] }}",
                       databricks_cluster=databricks_cluster_brnz),
    default_args=default_args,
    dag=dag,
)

load_silver_tasks = SubDagOperator(
    task_id="load_silver_tasks",
    subdag=load_subdag(parent_dag_name=DAG_NAME,
                       child_dag_name="silver",
                       args=default_args,
                       log_date="{{ dag_run.conf['logdate'] }}",
                       nb_path=GoogleAnalyticsOIDC.notebook_silver,
                       dataset_id="{{ dag_run.conf['dataset_id'] }}",
                       databricks_cluster=databricks_cluster_silver),
    default_args=default_args,
    dag=dag,
)

log_tardis = PythonOperator(
    task_id="log_tardis",
    python_callable=update_tardis,
    op_kwargs={
        'post_type': ['Data'],
        'logdate': "{{ dag_run.conf['logdate'] }}",
        'tardis_source': GoogleAnalyticsOIDC.tardis_data_source,
        'status': "Data Complete",
        'comments': "Backfill Completed with the GA load..!"
    },
    dag=dag
)

end_task = DummyOperator(
    task_id="end_task",
    trigger_rule="all_done",
    dag=dag
)

orchestration_task >> create_conn_db >> count_update_tardis >> trigger_dag_task >> end_task
orchestration_task >> load_bronze_tasks >> load_silver_tasks >> log_tardis >> end_task
