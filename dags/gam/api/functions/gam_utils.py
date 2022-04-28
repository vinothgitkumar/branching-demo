import json
import ast

import marshmellow.comms.slack_helpers as c

from plugins.config import AppConfig, GamAPIConfig, GamAPIdataDog, GAMLogConfig
import plugins.utilities.clusters.gam as clusters

from datetime import datetime


# ############## Common ##########################


def get_load_date(api_name):
    if 'run_date' in GamAPIConfig.api_reports[api_name].keys():
        dt = "{{ next_ds }}"
    else:
        dt = "{{ ds }}"
    return dt


def getDatabricksEnv(env):
    return 'prod' if env == 'production' else ('stg' if env == 'staging' else 'dev')


# ############## API related #####################


def getAPIReportDetails(api_name):
    r = GamAPIConfig.GAM_API_CONFIG_JSON
    # convert r into dic
    try:
        api_report = ast.literal_eval(r) if type(r) is str else r

        if api_name in api_report.keys():
            print(api_report[api_name])
            return api_report[api_name]
        elif api_name in GamAPIConfig.api_reports.keys():
            return GamAPIConfig.api_reports[api_name]
        else:
            raise Exception(f'Not found config for {api_name}')
    except Exception:
        print("Either config not present or wrong in Variable. Considering from default.")
        return GamAPIConfig.api_reports[api_name]


# ############Databricks
def getAPIBronzeParam(env, dt, api_name, market):
    notebook_task_param_bronze = {
                'libraries': clusters.getGamApiClusterLibs(),
                'notebook_task': {
                    'notebook_path': GamAPIConfig.api_bronze_notebook,
                    'base_parameters': {
                        'env': getDatabricksEnv(env),
                        'dt': dt,
                        'version': GamAPIConfig.GAM_API_VERSION,
                        'api_name': api_name,
                        'market': str(market)
                    }
                },
                'new_cluster': clusters.getGamAPICluster(GamAPIdataDog.GAM_CREDS_PATH, env)
            }
    return notebook_task_param_bronze


def getAPISilverParam(env, dt, api_name, market):
    notebook_task_param_silver = {
        'libraries': clusters.getGamApiClusterLibs(),
        'notebook_task': {
            'notebook_path': GamAPIConfig.api_silver_notebook,
            'base_parameters': {
                'env': getDatabricksEnv(env),
                'dt': dt,
                'api_name': api_name,
                'market': str(market)
            }
        },
        'new_cluster': clusters.getGamAPICluster(GamAPIdataDog.GAM_CREDS_PATH, env)
    }
    return notebook_task_param_silver


def getDataDogBronzeJson(env, api_name, report_detail, hr):
    notebook_task_param_bronze = {
        'libraries': clusters.getGamApiClusterLibs(),
        'notebook_task': {
            'notebook_path': GamAPIdataDog.datadog_bronze_notebook,
            'base_parameters': {
                'env': getDatabricksEnv(env),
                'dt': "{{ ds }}",
                'market': str(report_detail['market']),
                'version': GamAPIConfig.GAM_API_VERSION,
                'api_name': api_name,
                'hr': hr
            }
        },
        'new_cluster': clusters.getGamAPICluster(GamAPIdataDog.GAM_CREDS_PATH, env)
    }
    return notebook_task_param_bronze


def getDataDogSilverJson(env, api_name, report_detail, hr):
    notebook_task_param_silver = {
        'libraries': clusters.getGamApiClusterLibs(),
        'notebook_task': {
            'notebook_path': GamAPIdataDog.datadog_silver_notebook,
            'base_parameters': {
                'env': 'prd' if env == 'production' else ('stg' if env == 'staging' else 'dev'),
                'dt': "{{ ds }}",
                'market': str(report_detail['market']),
                'api_name': api_name,
                'hr': hr
            }
        },
        'new_cluster': clusters.getGamAPICluster(GamAPIdataDog.GAM_CREDS_PATH, env)
    }
    return notebook_task_param_silver

# Backfill


def validate_market(market):
    if market is None or market not in [str(i['network_code']) for i in GAMLogConfig.network_id_and_bucket_name_by_market.values()]:
        raise Exception("Invalid market")


def parse_date(date_text):
    try:
        return datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD :", date_text)


def validate_date(params):
    start_logdate = params.get('start_logdate')
    end_logdate = params.get('end_logdate')

    p_start_logdate = parse_date(start_logdate)
    p_end_logdate = parse_date(end_logdate)

    if p_start_logdate > p_end_logdate:
        raise Exception("Start date must be before End date")


def validate_params(**kwargs):
    validate_market(kwargs['market'])

    api_name = kwargs['api_name']
    if api_name not in GamAPIConfig.api_reports.keys():
        raise Exception("Invalid API name")

    validate_date(kwargs)

# ################### SLACK RELATED ############################


def send_slack_alert(comments, run_state, slack_token='',
                     slack_channel="#de-google-alerts-fail", additional_alertees=None, job_name=None, context=None):

    print("slack alerting ", additional_alertees, job_name, context)
    if context:
        dag = context.get('task_instance').dag_id
        exec_date = context.get('execution_date')
        log_url = context.get('task_instance').log_url
        owner = context.get('dag').default_args.get('owner')
        slack_client = c.Slack(slack_api_token=slack_token)
        if job_name:
            comments = f"Job Name: {job_name} \nComments: " + comments
        template = c.SlackMessageBlockBuilderTemplate().create_slack_message_template_2(
            slack_title=dag,
            alert_owner=f'@{owner}',
            run_state=run_state,
            log_date=context.get('ds'),
            log_url=log_url,
            databricks_workspace=AppConfig.environment,
            message_datetime=exec_date,
            additional_alertees=additional_alertees,
            additional_comments=comments)
        print(template)
        r = slack_client.post_message(slack_channel=slack_channel, message_blocks=json.dumps(template))
        print(r)
