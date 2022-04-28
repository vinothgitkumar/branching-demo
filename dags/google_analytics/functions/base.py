import json
import logging

# import dags.google_analytics.functions.slack_helpers as c
import plugins.config as config
from plugins.config import AppConfig
from plugins.utilities.tardis_utils import update_tardis
import marshmellow.comms.slack_helpers as c

logger = logging.getLogger("GoogleAnalytics")


def get_class(class_name=None):
    print(type(class_name))
    if type(class_name) is not str:
        expected_class = getattr(config, class_name.__name__)
    else:
        expected_class = getattr(config, class_name)
    logger.debug("expected_class", expected_class)
    return expected_class


def send_slack_alert(comments, run_state, slack_token='',
                     slack_channel="#de-google-alerts-fail", additional_alertees=None, job_name=None, context=None):
    print(additional_alertees, job_name, context)
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
            additional_comments=comments,
            log_date=context['ds'],
            additional_alertees=additional_alertees)
        print(template)
        slack_client.logger.setLevel(logging.DEBUG)
        r = slack_client.post_message(slack_channel=slack_channel, message_blocks=json.dumps(template))
        print(r)


def task_failure(comments, GoogleAnalyticClass=None, job_name=None, context=None):
    # print(context)
    TARDIS_PROCESS_SOURCE = None
    if GoogleAnalyticClass is None:
        dataset = context.get('dag_run').conf.get('dag_dataset')
        GoogleAnalyticClass = get_class(dataset['class_name'])
        TARDIS_PROCESS_SOURCE = GoogleAnalyticClass.tardis_process_source
        comments = comments + f"\nDataset ID: {GoogleAnalyticClass.project_info_project_id}"
    if context.get('exception'):
        error_string = str(context.get('exception'))
        error_message = error_string if len(error_string) < 500 else error_string[:500]
        comments = comments + f"\nException: {error_message}"
    send_slack_alert(comments=comments,
                     run_state='FAILED',
                     job_name=job_name,
                     slack_channel=GoogleAnalyticClass.failure_channel,
                     slack_token=GoogleAnalyticClass.slack_de_token,
                     additional_alertees=GoogleAnalyticClass.additional_alertee,
                     context=context)

    if TARDIS_PROCESS_SOURCE is not None:
        update_tardis(
            post_type=["Process"],
            tardis_source=TARDIS_PROCESS_SOURCE,
            logdate=context["ds"],
            status="FAILED",
            comments=comments)
