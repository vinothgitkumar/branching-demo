from plugins.utilities.tardis_utils import update_tardis
import logging
from airflow.operators.slack_operator import SlackAPIPostOperator
from plugins.config import SlackConfig, GA360apiReportConfig
from plugins.utilities.vault import vault_instance


def update_tardis_status(post_type, source, day, status, comments, context=None):
    logdate = context[day]
    update_tardis(post_type=post_type,
                  tardis_source=source,
                  logdate=logdate,
                  status=status,
                  comments=comments
                  )


def send_slack_message(context, channel: str, text: str):
    if SlackConfig.is_enabled is False:
        logging.info('slack is disabled')
        return

    alert = SlackAPIPostOperator(
        task_id='slack_test',
        channel=channel,
        token=vault_instance.get_secret(SlackConfig.token_key),
        text=text
    )
    return alert.execute(context=context)


def success_alert(context):
    message = """
            :white_check_mark: DAG Finished Successfully.
            *DAG*: {dag}
            *Execution Date*: {exec_date}
            *Logs*: <{log_url}| View logs here>
            """.format(
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url)
    return send_slack_message(context=context, channel=GA360apiReportConfig.alert_channel, text=message)


def failure_alert(context):
    message = """
            :red_circle: Task Failed.
            *Task*: {task}
            *DAG*: {dag}
            *Owners*: {owner}
            *Execution Date*: {exec_date}
            *Logs*: <{log_url}| View logs here>
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        owner=get_alert_tags(context['dag'].default_args['owner']),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url)
    return send_slack_message(context=context, channel=GA360apiReportConfig.failure_channel, text=message)


# Takes in a comma separated string of slack tags ("bob_evan, us_tereseg, some_other") and
# returns a string containing tags to be used in a slack api call (
# "<@bob_evan>, <@us_tereseg>, <@some_other>"). Returns empty string if no string is provided
def get_alert_tags(owner: str):
    slack_ids = owner.split(",")
    if slack_ids:
        # example: ["bob ","gary","  "] -> becomes -> ["<@bob>","<@gary>"]
        tags = ["<@" + s.strip() + ">" for s in slack_ids if s.strip() != ""]
        return ', '.join(tags)
    else:
        return ""
