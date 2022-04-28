from plugins.utilities.airflow_connections import create_gcp_connection, create_databricks_connection
import json
from pytz import timezone as time
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from plugins.config import YoutubeGlobal, AppConfig
from airflow.exceptions import AirflowException
import marshmellow.comms.slack_helpers as c
from plugins.utilities.tardis_utils import update_tardis, read_tardis
from airflow.api.common.experimental.trigger_dag import trigger_dag
import logging
from airflow.utils import timezone

logger = logging.getLogger("YoutubeGlobal Logs")


def create_airflow_connections_yt(gcp_connection_id, gcp_vault_key):
    print('Creating Airflow Connection for Databricks')
    create_databricks_connection()
    print('Creating Airflow Connection for GCP')
    create_gcp_connection(connection_id=gcp_connection_id, vault_key=gcp_vault_key, force_create=False)


def encrypt_vault_data(vault_data):
    # Convert teh vault data to binary string
    vault_data_b = json.dumps(vault_data).encode()
    # Get encrption key from config file
    key_obj = YoutubeGlobal.encryption_key
    f = Fernet(key_obj)
    encrypted_data = f.encrypt(vault_data_b)
    return str(encrypted_data)


def log_tardis(reports, market, status, comments, log_date, load_date):
    try:
        log_process_success(f"youtube_sensor_{market}", log_date)
        for report in reports:
            datasource = report + "_" + market
            print(f"Datasource to be update in Tardis - {datasource}")
            update_tardis(post_type=["Data"],
                          logdate=load_date,
                          tardis_source=datasource,
                          status=status,
                          comments=comments
                          )

    except Exception as e:
        if "Couldn't change status back to Data Received" in str(e):
            for report in reports:
                datasource = report + "_" + market
                print("*** Data Reloaded ***")
                print(f"Datasource to be update in Tardis - {datasource}")
                update_tardis(post_type=["Data"],
                              logdate=load_date,
                              tardis_source=datasource,
                              status="Data Reload",
                              comments="Data Refreshed at BQ end!"
                              )
        else:
            print(e)
            raise AirflowException(e)


def log_tardis_callback(report, market, status, comments, context=None):
    try:
        load_date = context['ti'].xcom_pull(key='_message_value', task_ids='yt_pubsub_sensor')
        datasource = report + "_" + market
        response = read_tardis(get_type='Data', tardis_source='["' + datasource + '"]', logdate=load_date)
        if response is not None:
            print(response)
            data_status = response['dataStatus']['results'][0]['status']
            print(f"Data status in Tardis is  -> {data_status}")
            if data_status not in ['Data Not Received']:
                print(f"Datasource to be updated in Tardis - {datasource}")
                update_tardis(post_type=["Data"],
                              logdate=load_date,
                              tardis_source=datasource,
                              status=status,
                              comments=comments
                              )
        send_slack_alert(comments,
                         'FAILED',
                         YoutubeGlobal.slack_de_token,
                         YoutubeGlobal.failure_channel,
                         additional_alertees=YoutubeGlobal.additional_alertee,
                         job_name=f"{report}_{market} silver load".upper(),
                         context=context)
    except Exception as e:
        raise AirflowException(e)


def get_load_date():
    curr_datetime = datetime.now()
    curr_date = curr_datetime.date()
    date_string = curr_date.strftime('%Y-%m-%d')
    print(f"Process log_date is {date_string}")
    return date_string


def get_tardis_process_status(process_source, log_date):
    response = read_tardis(get_type='Process', tardis_source='["' + process_source + '"]', logdate=log_date)
    print(f"Tardis Response -> {response}")
    if response is not None:
        process_status = response['processStatus']['results'][0]['status']
        if process_status in ['STARTED']:
            print("***** Tardis status already updated *****")
        else:
            handle_process_status(process_source, log_date)
    else:
        handle_process_status(process_source, log_date)


def log_process_failure(market, log_date, status, comments, context=None):
    try:
        process_source = f"youtube_sensor_{market}"
        print(f"Process source to be update in Tardis - {process_source}")
        update_tardis(post_type=["Process"],
                      logdate=log_date,
                      tardis_source=process_source,
                      status=status,
                      comments=comments
                      )
        send_slack_alert(comments,
                         'FAILED',
                         YoutubeGlobal.slack_de_token,
                         YoutubeGlobal.failure_channel,
                         additional_alertees=YoutubeGlobal.additional_alertee,
                         job_name=f"Youtube Sensor {market}".upper(),
                         context=context)
    except Exception as e:
        raise AirflowException(e)


def log_process_success(process_source, log_date):
    try:
        print(f"Process source to be update in Tardis - {process_source}")
        update_tardis(post_type=["Process"],
                      logdate=log_date,
                      tardis_source=process_source,
                      status="COMPLETE",
                      comments=f"{process_source} process completed successfully!"
                      )
    except Exception as e:
        raise AirflowException(e)


def send_slack_alert(comments, run_state, slack_token='',
                     slack_channel="#de-google-alerts-fail", additional_alertees=None, job_name=None, context=None):
    print("slack alerting ", additional_alertees, job_name, context)
    if context:
        dag = context.get('task_instance').dag_id
        exec_date = context.get('execution_date')
        log_url = context.get('task_instance').log_url
        owner = context.get('dag').default_args.get('owner')
        slack_client = c.Slack(slack_api_token=slack_token)
        slack_client.logger.setLevel(logging.DEBUG)
        if job_name:
            comments = f"Job Name: {job_name} \nComments: " + comments
        template = c.SlackMessageBlockBuilderTemplate().create_slack_message_template_2(
            slack_title=dag,
            alert_owner=f'@{owner}',
            run_state=run_state,
            log_url=log_url,
            databricks_workspace=AppConfig.environment,
            message_datetime=exec_date,
            log_date=context['ds'],
            additional_alertees=additional_alertees,
            additional_comments=comments)
        print(template)
        slack_client.logger.setLevel(logging.DEBUG)
        r = slack_client.post_message(slack_channel=slack_channel, message_blocks=json.dumps(template))
        print(r)


def get_actual_process_date():
    curr_date = datetime.now()
    delta_day = 2
    new_date = curr_date - timedelta(delta_day)
    run_date = new_date.date()
    date_string = run_date.strftime('%Y-%m-%d')
    print(f"The actual process date is {date_string}")
    return date_string


def retrigger_dag(reports, market, dag_name, load_date):
    for report in reports:
        datasource = report + "_" + market
        print(f"Datasource to be update in Tardis - {datasource}")
        update_tardis(post_type=["Data"],
                      logdate=load_date,
                      tardis_source=datasource,
                      status="Data Reload",
                      comments="Data reloaded because data refreshed message received..!!")
    trigger_dag(dag_id=dag_name,
                conf={})


def process_pubsub_message(reports, market, current_date, dag_name, actual_date, context=None):
    try:
        load_date = context['ti'].xcom_pull(key='_message_value', task_ids='yt_pubsub_sensor')
        print(f"The load date obtained from Message is : {load_date}")
        if load_date is None:
            raise AirflowException("No Value passed")
        print(f"The actual process date is --> {actual_date}")
        if str(load_date) != str(actual_date):
            log_process_success(f"youtube_sensor_{market}", current_date)
            retrigger_dag(reports,
                          market,
                          dag_name,
                          load_date)
        else:
            log_process_success(f"youtube_sensor_{market}", current_date)
            # log_tardis(reports,
            #            market,
            #            "Data Received",
            #            "Message received by PubSub Listener..!!",
            #            current_date,
            #            load_date)
    except Exception as e:
        raise AirflowException(e)


def handle_process_status(process_source, log_date):
    try:
        update_tardis(post_type=["Process"],
                      logdate=log_date,
                      tardis_source=process_source,
                      status="STARTED",
                      comments=f"Starting {process_source} process..")
    except Exception as e:
        if "Couldn't change status back to STARTED" in str(e):
            update_tardis(post_type=["Process"],
                          logdate=log_date,
                          tardis_source=process_source,
                          status="PARTIAL",
                          comments=f"{process_source} process will rerun as new pubsub message is recieved.")
            update_tardis(post_type=["Process"],
                          logdate=log_date,
                          tardis_source=process_source,
                          status="STARTED",
                          comments=f"Starting {process_source} process again as new pubsub message is pulled!!")


def trigger_group_content(**params):
    vault_string = params.get('vault_string')
    print("Triggering Youtube group content DAG")
    run_id = f"youtube_group_contents_{timezone.utcnow().isoformat()}"
    dag_inputs = {'vault_string': vault_string}
    trigger_dag(dag_id="youtube_group_contents",
                run_id=run_id,
                conf=json.dumps(dag_inputs),
                replace_microseconds=False)
    print(
        f"Successfully triggered dag ***** youtube_group_contents *****"
        f"with run_id = {run_id}")


def log_tardis_failure_callback(load_date, status, source, comments, context=None):
    try:
        datasource = source
        print(f"Datasource to be update in Tardis - {datasource}")
        update_tardis(post_type=["Data"],
                      logdate=load_date,
                      tardis_source=datasource,
                      status=status,
                      comments=comments
                      )
        send_slack_alert(comments,
                         'FAILED',
                         YoutubeGlobal.slack_de_token,
                         YoutubeGlobal.failure_channel,
                         additional_alertees=YoutubeGlobal.additional_alertee,
                         job_name=f"{datasource} load".upper(),
                         context=context)
    except Exception as e:
        raise AirflowException(e)


def log_tardis_success_callback(load_date, status, source, comments, context=None):
    try:
        datasource = source
        print(f"Datasource to be update in Tardis - {datasource}")
        update_tardis(post_type=["Data"],
                      logdate=load_date,
                      tardis_source=datasource,
                      status=status,
                      comments=comments
                      )

    except Exception as e:
        if "Couldn't change status back to Data Received" in str(e):
            datasource = source
            print("*** Data Reloaded ***")
            print(f"Datasource to be update in Tardis - {datasource}")
            update_tardis(post_type=["Data"],
                          logdate=load_date,
                          tardis_source=datasource,
                          status="Data Reload",
                          comments="Data Refreshed hence reloading the data"
                          )
        else:
            print(e)
            raise AirflowException(e)


def read_audit_log(log_date, source):
    response = read_tardis(get_type='AuditLog',
                           tardis_source='["' + source + '"]',
                           logdate=log_date)
    return response['auditLog']['results']


def update_audit_data_status(log_date, status, source, trending_schedule, comments, context=None):
    response = read_audit_log(log_date,
                              source)
    print(f"Trending feed ran for {len(response)} times")
    if len(response) == trending_schedule:
        log_tardis_success_callback(log_date,
                                    status,
                                    source,
                                    comments)

    else:
        print("Trending feed Tardis status updated already")


def fetch_trending_time():
    trending_time = datetime.now(time('EST')).strftime('%H') + "_00"
    return trending_time


def get_trending_schedule(schedule):
    num_schedule = schedule.split(" ")[1].split(",")
    return len(num_schedule)
