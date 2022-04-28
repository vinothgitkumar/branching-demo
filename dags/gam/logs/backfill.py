from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from plugins.config import GAMLogConfig
from dags.gam.logs.functions.backfill_utils import validate_params, trigger_job

DAG_NAME = 'gam_logs_backfill'

# ======================================
# Mandatory parameters - start_logdate, end_logdate, market, report_name
# If only one logdate, pass it to end_logdate and pass None for start_logdate
# end_logdate should be equal or greater than start_logdate
# ======================================
# Example config:
# {"start_logdate":"2021-10-01","end_logdate":"2021-11-01","market":"Russia","report_name":"NetworkRequests"}
# {"start_logdate":null,"end_logdate":"2021-04-02","market":"Russia","report_name":"NetworkClicks"}


default_args = {
    'owner': GAMLogConfig.GAM_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(dag_id=DAG_NAME,
          max_active_runs=10,
          schedule_interval=None,
          default_args=default_args,
          catchup=False,
          )

validate_param = PythonOperator(task_id="validate_params",
                                python_callable=validate_params,
                                op_kwargs={
                                    'start_logdate': "{{ dag_run.conf['start_logdate'] }}",
                                    'end_logdate': "{{ dag_run.conf['end_logdate'] }}",
                                    'market': "{{ dag_run.conf['market'] }}",
                                    'report_name': "{{ dag_run.conf['report_name'] }}"
                                },
                                dag=dag
                                )

trigger_task = PythonOperator(task_id="trigger_task",
                              python_callable=trigger_job,
                              op_kwargs={
                                  'start_logdate': "{{ dag_run.conf['start_logdate'] }}",
                                  'end_logdate': "{{ dag_run.conf['end_logdate'] }}",
                                  'market': "{{ dag_run.conf['market'] }}",
                                  'report_name': "{{ dag_run.conf['report_name'] }}",
                                  'read_bkp_bucket': "{{ dag_run.conf['read_bkp_bucket'] }}",
                                  'bronze_notebook': "{{ dag_run.conf['bronze_notebook'] }}"
                              },
                              dag=dag
                              )

validate_param >> trigger_task
