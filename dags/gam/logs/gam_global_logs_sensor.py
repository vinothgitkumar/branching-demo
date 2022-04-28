from airflow.operators import DummyOperator, PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from functools import partial
from plugins.config import GAMLogConfig as config
from dags.gam.logs.functions.utils import trigger_databricks_dag, alert, initialise_tardis_data_status, create_airflow_connections_ga, send_slack_alert
from dags.gam.logs.functions.GCSSensorOperator import GAMLogsSensor


default_args = {
    'owner': config.GAM_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


def get_tardis_initialise_operator(report_name, network_code):
    return PythonOperator(
        task_id=f"initialise_tardis_{report_name}",
        python_callable=initialise_tardis_data_status,
        op_kwargs={
            'report_name': report_name,
            'network_code': network_code,
            'logdate': "{{ds}}"
        },
        retries=3,
        provide_context=True,
        retry_delay=timedelta(minutes=2)
    )


def get_gamlogs_sensor(report_name, network_code, network_id, gam_source_bucket):
    return GAMLogsSensor(
        task_id=f"gam_sensor_{report_name}",
        bucket=gam_source_bucket,
        network_id=network_id,
        network_code=network_code,
        report_name=report_name,
        log_date="{{ds_nodash}}",
        google_cloud_conn_id=config.GAM_CONNECTION,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=10),
        mode='reschedule',
        poke_interval=60 * 5,
        on_success_callback=partial(alert, status="Data Received",
                                    comment="GAM Data files Ready to load!! ", report_name=report_name,
                                    network_code=network_code),
        on_failure_callback=partial(alert, status="Data Validation Failed",
                                    comment="GAM Global Sensor Failure!! ", report_name=report_name,
                                    network_code=network_code, slack_source_name="GAM Global Sensor Failed")
    )


def get_databricks_trigger_operator(report_name, network_code, network_id):
    return PythonOperator(
        task_id=f"trigger_databricks_{report_name}",
        python_callable=trigger_databricks_dag,
        op_kwargs={
            'report_name': report_name,
            'network_id': network_id,
            'network_code': network_code

        },
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=partial(alert, status="Data Validation Failed",
                                    comment="Data Not Available in Delta Tables!! ", report_name=report_name,
                                    network_code=network_code, slack_source_name="GAM Global Trigger Databricks Failed")
    )


for market_name, market_values in config.network_id_and_bucket_name_by_market.items():
    dag_name = f'gam_global_sensor_{market_name}'
    with DAG(dag_name,
             max_active_runs=1,
             default_args=default_args,
             catchup=False,
             schedule_interval=config.GAM_GLOBAL_SENSOR_DAG_SCHEDULE,
             on_success_callback=partial(send_slack_alert,
                                         'GAM Sensor Dag success',
                                         'Success',
                                         config.slack_de_token,
                                         config.alert_channel,
                                         None,
                                         None
                                         )
             ) as dag:

        start_task = DummyOperator(task_id='start_task')

        create_conn_db = PythonOperator(
            task_id="create_conn_db",
            python_callable=create_airflow_connections_ga,
            op_kwargs={
                'gcp_connection_id': config.GAM_CONNECTION,
                'gcp_vault_key': config.GAM_CRED_SECRET_PATH
            },
            dag=dag
        )

        end_task = DummyOperator(
            task_id='end_task',
            trigger_rule='all_success')

        network_id = market_values['network_id']
        network_code = market_values['network_code']
        gam_source_bucket = market_values['gam_source_bucket']
        reports = config.gam_log_types[network_id]
        for report_name in reports.keys():
            if reports[report_name] == 'active':
                initialise_tardis_task = get_tardis_initialise_operator(report_name, network_code)
                sensor_task = get_gamlogs_sensor(report_name, network_code, network_id, gam_source_bucket)
                trigger_databricks = get_databricks_trigger_operator(report_name, network_code, network_id)

                start_task >> create_conn_db >> initialise_tardis_task >> sensor_task >> trigger_databricks >> end_task
    globals()[dag_name] = dag
