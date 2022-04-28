import datetime

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.hooks.databricks_hook import DatabricksHook
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from urllib.parse import quote
from airflow.configuration import conf
import concurrent.futures


class AlertingDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):

    def __init__(self, alert_config, alert_polling_enabled, **kwargs):
        self.parent = super().__init__(**kwargs)
        self.alert_config = alert_config
        """
        How long the alert loop will wait before retrying
        the retrieval of the Job's Status (Is the job in a "RUNNING" state?)
        """
        self.job_start_poll_interval = 30
        """
        Whether the alert loop is enabled
        """
        self.alert_polling_enabled = alert_polling_enabled
        """
        How long the alert loop will wait before retrying
        the retrieval of the Job's Run ID
        """
        self.alert_run_id_wait_time = 30

        if alert_polling_enabled is None:
            self.alert_polling_enabled = True

    def execute(self, context):
        thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        execute_future = thread_pool.submit(super().execute, context)
        futures = [execute_future]
        if self.alert_polling_enabled is True:
            self.log.info("Alert Polling is enabled, adding future")
            alert_future = thread_pool.submit(self.alert_loop, context)
            futures.append(alert_future)

        self.log.info("Initializing Futures Pool {}".format(futures))
        """
        We'll block on the polling loop until one of our futures throws an exception
        """
        try:
            # concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)

            for f in futures:
                if f.running():
                    self.log.info("Polling Future [{}]'s Result; Waiting 30 Seconds".format(f))
                    f.result(timeout=30)

        except Exception as e:
            self.log.info("Exception [{}] was throw, destroying running future(s)".format(e))

            """
            Cancel all running futures
            """
            for f in futures:
                while not f.done():
                    f.cancel()
            thread_pool.shutdown(wait=False)
            """
            Re-throw the future's original exception
            """
            raise e

    def alert_loop(self, context):

        execution_date = context.get("execution_date")
        self.log.info("Instantiating Databricks Hook For Connection [%s]", self.databricks_conn_id)

        hooks = DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

        self.log.info("Instantiating Databricks Hook For Connection [%s]", self.databricks_conn_id)
        while self.run_id is None:
            time.sleep(self.polling_period_seconds)

        self.log.info('Run ID [%s].', self.run_id)
        self.log.info("Execution Date [%s].", execution_date)

        run_page_url = hooks.get_run_page_url(self.run_id)
        run_state = hooks.get_run_state(self.run_id)

        def validate_alert_config(alert_config):
            keys = ["token", "alert_titles", "channel", "alert_polling_interval"]
            for k in alert_config.keys():
                if k in keys:
                    keys.pop(keys.index(k))
                _v = alert_config.get(k, None)
                if _v is None:
                    raise Exception("Alert Config [{}] is misconfigured [{}] [{}]; "
                                    "value cannot be None".format(alert_config, k, _v))

            if len(keys) != 0:
                raise Exception("Alert Config [{}] is misconfigured, it must contain all valid keys [{}]"
                                .format(alert_config, keys))

        validate_alert_config(self.alert_config)
        slack_token = self.alert_config.get("token")
        slack_title_scheduler = self.alert_config.get("alert_titles")[0]
        slack_title_poller = self.alert_config.get("alert_titles")[1]
        slack_channel = self.alert_config.get("channel")
        slack_failure_channel = self.alert_config.get("failure_channel", self.alert_config.get("channel"))
        alert_polling_interval = self.alert_config.get("alert_polling_interval", (60 * 60))
        slack_alert_owner = self.alert_config.get("owner", "@us_mchamber")
        slack_alert_environment = self.alert_config.get("databricks_workspace", "unknown")

        client = WebClient(token=slack_token)
        self.log.info('[Alerting Loop]: Log URL [%s] .', str(self.log_url(execution_date=execution_date)))

        try:
            # Reference: https://app.slack.com/block-kit-builder
            response = client.chat_postMessage(channel=slack_channel,
                                               blocks=self._create_slack_message(slack_title_scheduler,
                                                                                 slack_alert_owner, run_state,
                                                                                 run_page_url, execution_date,
                                                                                 slack_alert_environment)
                                               )

            self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(response.status_code))
        except SlackApiError as e:
            self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(e))

        while True:
            if run_state.life_cycle_state in ['RUNNING']:
                """
                Good State, decorate our message such that we know the job "started running"
                successfully
                """
                try:
                    response = client.chat_postMessage(channel=slack_channel,
                                                       blocks=self._create_slack_message(slack_title_scheduler,
                                                                                         slack_alert_owner, run_state,
                                                                                         run_page_url, execution_date,
                                                                                         slack_alert_environment)
                                                       )

                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(response.status_code))
                except SlackApiError as e:
                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(e))
                break
            elif run_state.is_terminal:
                """
                Bad State, decorate our message such that we know this wasn't a good thing
                """
                try:
                    # Reference: https://app.slack.com/block-kit-builder
                    response = client.chat_postMessage(channel=slack_failure_channel,
                                                       blocks=self._create_slack_message(slack_title_scheduler,
                                                                                         slack_alert_owner, run_state,
                                                                                         run_page_url, execution_date,
                                                                                         slack_alert_environment)
                                                       )

                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(response.status_code))
                except SlackApiError as e:
                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(e))
                raise Exception("Databricks Job Invalid State")

            self.log.info('[Alerting Loop] Waiting for job to start; State [%s] .', run_state)

            time.sleep(self.job_start_poll_interval)
            run_state = hooks.get_run_state(self.run_id)

        """
        Polling Loop
        """
        while True:
            run_state = hooks.get_run_state(self.run_id)
            self.log.info('[Alerting Loop] URL [%s].', run_page_url)
            self.log.info('[Alerting Loop] State [%s] .', run_state)
            if run_state.life_cycle_state in ['RUNNING']:
                """
                Good State, decorate our message such that we know the job "is still running"
                successfully
                """
                try:
                    response = client.chat_postMessage(channel=slack_channel,
                                                       blocks=self._create_slack_message(slack_title_poller,
                                                                                         slack_alert_owner, run_state,
                                                                                         run_page_url, execution_date,
                                                                                         slack_alert_environment)
                                                       )

                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(response.status_code))
                except SlackApiError as e:
                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(e))
            elif run_state.is_terminal:
                """
                Bad State, decorate our message such that we know this wasn't a good thing
                """
                try:
                    response = client.chat_postMessage(channel=slack_failure_channel,
                                                       blocks=self._create_slack_message(slack_title_poller,
                                                                                         slack_alert_owner, run_state,
                                                                                         run_page_url, execution_date,
                                                                                         slack_alert_environment)
                                                       )

                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(response.status_code))
                except SlackApiError as e:
                    self.log.info('[Alerting Loop]: Slack Response Status Code [%s] .', str(e))
                raise Exception("Databricks Job Invalid State")

            self.log.info('[Alerting Loop] Sleeping for %s seconds.', alert_polling_interval)
            time.sleep(alert_polling_interval)

    def log_url(self, execution_date):

        iso = quote(datetime.date.today().isoformat())

        if execution_date is not None:
            iso = quote(execution_date.isoformat())

        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + f"/log?execution_date={iso}&task_id={self.task_id}&dag_id={self.dag_id}"

    def _create_slack_message(self, slack_title, alert_owner, run_state, run_page_url, execution_date,
                              databricks_workspace="unknown"):

        markdown_str = "*Date :* " + str(datetime.date.today()) \
                       + "\n*Owner:* " + alert_owner \
                       + "\n*Environment:* " + databricks_workspace + "\n*Job Status:* ```" + str(run_state) + "```"

        return [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": slack_title
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": markdown_str,
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Databricks Job Run",
                        },
                        "url": run_page_url
                    }
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Astronomer Log",
                        },
                        "url": self.log_url(execution_date=execution_date)
                    }
                ]
            }
        ]
