import pytz
import json
from airflow.utils import timezone
from datetime import datetime
from plugins.utilities.tardis_utils import read_tardis, update_tardis
from airflow.api.common.experimental.trigger_dag import trigger_dag
from plugins.config import GAMLogConfig
from plugins.utilities.airflow_connections import create_gcp_connection, create_databricks_connection
import marshmellow.comms.slack_helpers as c
from plugins.config import AppConfig
import logging

logger = logging.getLogger("GoogleAdManager Logs")


def hour_exists(date_hour, timezone="US/Pacific"):
    """
    Returns false if the hour does not exists due to DST, true otherwise.
    """
    try:
        timezone = pytz.timezone(timezone)
        timezone.localize(datetime.strptime(date_hour, '%Y%m%d%H'), is_dst=None)
    except pytz.NonExistentTimeError:
        # Due to DST, we will never get a GAM file for this hour.
        return False
    except pytz.AmbiguousTimeError:
        # Due to DST, one hour would be repeated, GAM has sent file properly for this hour till now
        pass
    return True


def file_processed(file_name, last_updated_time, network_code):
    log_date = file_name.split("_")[2]
    file_source_name = file_name.split("_")[0]
    tardis_source = generate_tardis_source(file_source_name, network_code)
    print("Check if the file is already processed")
    tardis_updated_ts = get_audit_record(tardis_source, file_name, log_date)
    if tardis_updated_ts is None:
        return False

    print('File Name: ', file_name, 'Corrected File last modified time: ', last_updated_time,
          " Processed time in Taris: ", tardis_updated_ts)
    if tardis_updated_ts < last_updated_time:
        return False
    else:
        return True


def generate_tardis_source(report_name, network_code):
    return "gam." + report_name + "_" + str(network_code)


def tardis_update_for_corrected_files(file_name, network_code, last_updated_time):
    """
        param file_name
        return
    """

    log_date = file_name.split("_")[2]
    log_date_formatted = datetime.strptime(log_date, '%Y%m%d').strftime('%Y-%m-%d')
    file_source_name = file_name.split("_")[0]
    tardis_source = generate_tardis_source(file_source_name, network_code)
    additional_info = file_name + "|" + str(last_updated_time)
    update_tardis(post_type=['AuditLog'],
                  tardis_source=tardis_source,
                  logdate=log_date_formatted,
                  status="Data Received",
                  additionalInfo=additional_info)
    update_tardis(post_type=['Data'],
                  tardis_source=tardis_source,
                  status='Data Reload',
                  logdate=log_date_formatted,
                  comments="Marked Data Reloaded due to corrected files")


def get_audit_record(tardis_source, file_name, log_date):
    """
    param tardis_source
    param file_name
    return
    """

    log_date_formatted = datetime.strptime(log_date, '%Y%m%d').strftime('%Y-%m-%d')
    response = read_tardis(get_type='AuditLog', tardis_source='["' + tardis_source + '"]', logdate=log_date_formatted)
    print("response:", response)
    update_ts = None
    res = response['auditLog']['results']
    # first time scenerio, then return current timestamp (as it has to be proccessed
    if len(response['auditLog']['results']) == 0:
        return update_ts
    found = False
    for r in res:
        print("r['additionalInfo']:", r['additionalInfo'])
        if file_name in r['additionalInfo']:
            temp_ts = datetime.fromisoformat(r['additionalInfo'].split('|')[1])
            found = True
        if found:
            if update_ts is None:
                update_ts = temp_ts
            else:
                update_ts = temp_ts if temp_ts > update_ts else update_ts
    if not found:
        return update_ts
    return update_ts


def trigger_databricks_dag(**kwargs):
    ti = kwargs["ti"]
    report_name = kwargs["report_name"]
    network_id = kwargs['network_id']
    network_code = kwargs['network_code']
    sensor_map = ti.xcom_pull(task_ids=f"gam_sensor_{report_name}")
    trigger_bronze_dag(sensor_map, report_name, network_id, network_code)


def trigger_bronze_dag(logdate_files_map, report_name, network_id, network_code, read_bkp_bucket='false',
                       bronze_notebook="gcs_to_delta_bronze"):
    for log_date in logdate_files_map.keys():
        run_id = f"{network_code}_{log_date}_{report_name}_{network_id}_{timezone.utcnow().isoformat()}"
        print(run_id)
        instance = GAMLogConfig.databricks_instance_types[network_id][report_name]
        files = '|'.join([file for file in logdate_files_map[log_date]])
        dag_inputs = {'files': files,
                      'instance': instance,
                      'report_name': report_name,
                      'log_date': log_date,
                      'network_id': network_id,
                      'network_code': network_code,
                      'read_bkp_bucket': read_bkp_bucket,
                      'bronze_notebook': bronze_notebook}
        trigger_dag(dag_id="gam_logs_global_bronze",
                    run_id=run_id,
                    conf=json.dumps(dag_inputs),
                    replace_microseconds=False)
        print(
            f"Successfully triggered dag for {report_name} for {network_code} with log date {log_date} and run_id = {run_id}")


def alert(context, status, comment, network_code=0, report_name='', slack_source_name='', log_date=None):
    source_status = ''
    if network_code == 0 and report_name == '':
        network_code = context['run_id'].split('_')[0]
        log_date = context['run_id'].split('_')[1]
        report_name = context['run_id'].split('_')[2]

    log_date_formatted = context['ds'] if log_date is None else datetime.strptime(log_date, '%Y%m%d').strftime(
        '%Y-%m-%d')
    source = generate_tardis_source(report_name, network_code)

    response = read_tardis(get_type='Data', tardis_source='["' + source + '"]', logdate=log_date_formatted)
    if response != '':
        source_status = response["dataStatus"]["results"][0]['status']['status']
    if source_status.lower() != status.lower():
        update_tardis(post_type=['Data'],
                      tardis_source=source,
                      status=status,
                      logdate=log_date_formatted,
                      comments=comment)
    else:
        print("Entry already exist!!")

    if status.lower() == 'Data Validation Failed'.lower():
        send_slack_alert(comment,
                         'FAILED',
                         GAMLogConfig.slack_de_token,
                         GAMLogConfig.failure_channel,
                         additional_alertees=GAMLogConfig.additional_alertee,
                         context=context,
                         log_date=log_date_formatted,
                         job_name=slack_source_name + " for network code - " + str(
                             network_code) + " and report type - " + report_name)


def initialise_tardis_data_status(**kwargs):
    update_tardis(post_type=['Data'],
                  tardis_source=generate_tardis_source(kwargs['report_name'], kwargs['network_code']),
                  status="Data Not Received",
                  logdate=kwargs['logdate'])


def set_dag_conf(context, dag_run_obj):
    dag_conf = context['dag_run'].conf
    dag_inputs = {
        'instance': dag_conf['instance'],
        'report_name': dag_conf['report_name'],
        'log_date': dag_conf['log_date'],
        'network_id': dag_conf['network_id'],
        'network_code': dag_conf['network_code']
    }
    dag_run_obj.payload = dag_inputs
    dag_run_obj.run_id = context['dag_run'].run_id
    return dag_run_obj


def create_airflow_connections_ga(gcp_connection_id, gcp_vault_key):
    logger.info('Creating Airflow Connection for Databricks')
    create_databricks_connection()
    logger.info('Creating Airflow Connection for GCP')
    create_gcp_connection(connection_id=gcp_connection_id, vault_key=gcp_vault_key, force_create=False)


def send_slack_alert(comments, run_state, slack_token='',
                     slack_channel="#de-google-alerts-fail", additional_alertees=None, job_name=None, context=None,
                     log_date=None):
    print("slack alerting ", additional_alertees, job_name, context)
    if context:
        dag = context.get('task_instance').dag_id
        exec_date = context.get('execution_date')
        log_url = context.get('task_instance').log_url
        owner = context.get('dag').default_args.get('owner')
        log_date = context.get('ds') if log_date is None else log_date
        slack_client = c.Slack(slack_api_token=slack_token)
        slack_client.logger.setLevel(logging.DEBUG)
        if job_name:
            comments = f"Job Name: {job_name} \nComments: " + comments
        template = c.SlackMessageBlockBuilderTemplate().create_slack_message_template_2(
            slack_title=dag,
            alert_owner=f'@{owner}',
            run_state=run_state,
            log_date=log_date,
            log_url=log_url,
            databricks_workspace=AppConfig.environment,
            message_datetime=exec_date,
            additional_alertees=additional_alertees,
            additional_comments=comments)
        print(template)
        slack_client.logger.setLevel(logging.DEBUG)
        r = slack_client.post_message(slack_channel=slack_channel, message_blocks=json.dumps(template))
        print(r)


def gam_gold_alerts(context, source, status, comments='', slack_source_name=''):
    rundate = context['ds']
    print("in alert:", source, rundate, status, comments)
    try:
        update_tardis(post_type=['Data'],
                      tardis_source=source,
                      status=status,
                      logdate=rundate,
                      comments=comments)
    except Exception as ex:
        print('Exception occurred during tardis update in ', ex)

    if status.lower() == 'Data Validation Failed'.lower() or status.lower() == 'Data Not Received'.lower():
        send_slack_alert(comments,
                         'FAILED',
                         GAMLogConfig.slack_de_token,
                         GAMLogConfig.failure_channel,
                         additional_alertees=GAMLogConfig.additional_alertee,
                         context=context,
                         job_name=slack_source_name + " for report - " + str(source))
