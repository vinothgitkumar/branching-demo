import datetime
import logging
from typing import AnyStr, Dict, Optional, Sequence, Union

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.models.blocks import Block
from slack_sdk.web import SlackResponse


class Slack:
    def __init__(self, slack_api_token: AnyStr, slack_url: Optional[AnyStr] = "https://www.slack.com/api/"):
        if slack_api_token is None or \
                not isinstance(slack_api_token, str) or \
                (isinstance(slack_api_token, str) and len(slack_api_token) == 0):
            raise ValueError("Slack object must be instantiated "
                             "with a non None/empty string value for the "
                             "variable slack_api_token ")

        self._slack_api_token = slack_api_token
        self._slack_client = WebClient(
            base_url=slack_url, token=self.slack_api_token)
        self._logger = logging.getLogger("Slack-Logger")

    @property
    def slack_api_token(self) -> AnyStr:
        return self._slack_api_token

    @property
    def slack_client(self) -> WebClient:
        return self._slack_client

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def test_connection(self) -> SlackResponse:
        return self._slack_client.api_test()

    def post_message(self, slack_channel: AnyStr, message_blocks: Optional[Sequence[Union[Dict, Block]]]):
        if slack_channel is None or \
                not isinstance(slack_channel, str) or \
                (isinstance(slack_channel, str) and len(slack_channel) == 0):
            raise ValueError("slack_channel must be non None/empty string")

        slack_response = None
        try:
            self._logger.debug(
                "Attempting to chat to topic {}".format(slack_channel))
            slack_response = self._slack_client.chat_postMessage(
                channel=slack_channel, blocks=message_blocks)
        except SlackApiError as e:
            self._logger.error("post_message threw SlackApiError {}".format(e))

        return slack_response


class SlackMessageBlockBuilderTemplate:
    """
    Convenience class usage, the return values of these methods can be passed to
    Slack("api token").post_message("sample channel" SlackMessageBlockBuilderTemplate.create...)
    """

    @staticmethod
    def create_slack_message_template_0(slack_title: AnyStr,
                                        alert_owner: AnyStr,
                                        run_state: AnyStr,
                                        run_page_url: Optional[AnyStr],
                                        log_url: Optional[AnyStr],
                                        databricks_workspace: AnyStr = "unknown",
                                        message_datetime=datetime.date.today()):
        markdown_str = "*Date :* " + str(message_datetime) \
                       + "\n*Owner:* " + alert_owner \
                       + "\n*Environment:* " + databricks_workspace + \
                       "\n*Job Status:* ```" + str(run_state) + "```"

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
                        "url": log_url
                    }
                ]
            }
        ]

    @staticmethod
    def create_slack_message_template_1(slack_title: AnyStr,
                                        alert_owner: AnyStr,
                                        run_state: AnyStr,
                                        run_page_url: Optional[AnyStr],
                                        log_url: Optional[AnyStr],
                                        databricks_workspace: AnyStr = "unknown",
                                        additional_alertees: AnyStr = "",
                                        message_datetime=datetime.date.today()):
        markdown_str = f"*Date:* {str(message_datetime)}" \
                       + f"\n*Environment:* {databricks_workspace}" \
                       + f"\n*Owner:* {alert_owner}"

        if run_state.life_cycle_state in ["RUNNING"]:
            emoji = ":runner:"
        elif run_state.is_successful:
            emoji = ":white_check_mark:"
        elif run_state.is_terminal:
            emoji = ":x:"
            # include addl alertees if there's a failure
            if additional_alertees:
                markdown_str += f"\n*Additional Alertees:* {additional_alertees}"
        markdown_str += f"\n*Job Status:* ```{str(run_state)}```"

        return [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {slack_title}",
                    "emoji": True
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
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Astronomer Log",
                        },
                        "url": log_url
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]

    @staticmethod
    def create_slack_message_template_2(slack_title: AnyStr,
                                        alert_owner: AnyStr,
                                        run_state: AnyStr,
                                        run_page_url: Optional[AnyStr],
                                        log_url: Optional[AnyStr],
                                        databricks_workspace: AnyStr = "unknown",
                                        additional_alertees: AnyStr = "",
                                        additional_comments: AnyStr = "",
                                        message_datetime=datetime.date.today()):

        markdown_str = f"*Date:* {str(message_datetime)}" \
                       + f"\n*Environment:* {databricks_workspace}"
        emoji = ":confused:"
        if run_state.lower() in ["running"]:
            emoji = ":runner:"
            markdown_str += f"\n*Owner:* {alert_owner}"
        elif run_state.lower() in ["is_successful", "success"]:
            emoji = ":white_check_mark:"
        elif run_state.lower() in ["is_terminal", "failed"]:
            emoji = ":x:"
            markdown_str += f"\n*Owner:* {alert_owner}"
            # include addl alertees if there"s a failure
            if additional_alertees:
                markdown_str += f"\n*Additional Alertees:* {','.join(additional_alertees)}"
        elif run_state.lower() in ["retry", "retrying"]:
            emoji = ":large_yellow_circle:"
            markdown_str += f"\n*Owner:* {alert_owner}"
            if additional_alertees:
                markdown_str += f"\n*Additional Alertees:* {','.join(additional_alertees)}"

        markdown_str += f"\n*Job Status:* {str(run_state)}"
        if additional_comments:
            markdown_str += f"\n*Additional Comments:* ```{str(additional_comments)}```"

        return [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {slack_title}",
                    "emoji": True
                },
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
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Astronomer Log",
                        },
                        "url": log_url
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
