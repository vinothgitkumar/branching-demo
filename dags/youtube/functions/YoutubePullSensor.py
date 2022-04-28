from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import base64
import json


class YoutubeHandleMessage:
    def decrypt_msg(pulled_message):
        msg = json.loads(base64.b64decode(pulled_message[0]["message"]["data"])
                         .decode("utf-8"))
        print("****" * 10, "\nPubSub msg: ")
        print(msg)
        print("****" * 10)
        return msg

    def flatten(msg):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
        flatten(msg)
        return out

    def get_actual_run_date(msg):
        if msg:
            date_timestamp = msg['runTime']
            datetime_obj = datetime.strptime(date_timestamp, '%Y-%m-%dT%H:%M:%S%z')
            load_date = datetime_obj.date()
            print(f"The data to be loaded for -> {load_date}")
            return load_date
        else:
            return None

    def handle_message(msg, flatten_msg, keys, values):
        # flatten_msg = flatten(msg)
        print(flatten_msg)
        msg_keys = msg.keys()
        key_list = []
        value_list = []
        flatten_keys = flatten_msg.keys()
        actual_keys = []

        for i in msg_keys:
            for j in keys:
                if i != j:
                    actual_keys.append(i + '_' + j)
                else:
                    actual_keys.append(i)
        #             print(actual_keys)
        actual_keys = [x for x in actual_keys if x in flatten_keys]
        print(actual_keys)

        for i in flatten_msg:
            if i in actual_keys:
                key_list.append(i)
        if len(key_list) != len(keys):
            print("No expected keys in Pulled message")
            return False, True
        else:
            for i in key_list:
                if flatten_msg[i] in values:
                    #               print("yes")
                    print(f"{i} : {flatten_msg[i]}")
                    value_list.append(flatten_msg[i])
                else:
                    print("This is not an appropriate msg")
                    return False, True
            if len(value_list) == len(values):
                print("Msg found!")
                return True, True
            else:
                return False, False


class YoutubePullSensor(BaseSensorOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.

    This sensor operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the poke method's criteria will be fulfilled and the messages will be
    returned from the operator and passed through XCom for downstream tasks.

    """
    # template_fields = ['keys', 'values']

    @apply_defaults
    def __init__(
            self,
            keys,
            values,
            project,
            subscription,
            max_messages,
            ack_messages=False,
            gcp_conn_id='youtube_gcp_connection',
            return_immediately=False,
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param project: the GCP project ID for the subscription (templated)
        :type project: str
        :param subscription: the Pub/Sub subscription name. Do not include the
            full subscription path.
        :type subscription: str
        :param max_messages: The maximum number of messages to retrieve per
            PubSub pull request
        :type max_messages: int
        :param return_immediately: If True, instruct the PubSub API to return
            immediately if no messages are available for delivery.
        :type return_immediately: bool
        :param ack_messages: If True, each message will be acknowledged
            immediately rather than by any downstream tasks
        :type ack_messages: bool
        :param gcp_conn_id: The connection ID to use connecting to
            Google Cloud Platform.
        :type gcp_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request
            must have domain-wide delegation enabled.
        :type delegate_to: str
        """
        super(YoutubePullSensor, self).__init__(*args, **kwargs)
        self.keys = keys
        self.values = values
        self.project = project
        self.subscription = subscription
        self.max_messages = max_messages
        self.ack_messages = ack_messages
        self.gcp_conn_id = gcp_conn_id
        self.return_immediately = return_immediately
        self.delegate_to = delegate_to
        self._messages = None

    def execute(self, context):
        """Overridden to allow messages to be passed"""
        super(YoutubePullSensor, self).execute(context)
        return self._messages

    def poke(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)
        pulled_message = hook.pull(
            self.project, self.subscription, self.max_messages,
            self.return_immediately)

        if pulled_message:
            msg = YoutubeHandleMessage.decrypt_msg(pulled_message)
            flatten_msg = YoutubeHandleMessage.flatten(msg)
            message_result, ack_check_flag = YoutubeHandleMessage.handle_message(msg, flatten_msg, self.keys,
                                                                                 self.values)
            print(f"The pulled message is as expected : {message_result}")

            if ack_check_flag:
                if message_result and self.ack_messages:
                    print("Getting load_date from pulled message")
                    run_date = YoutubeHandleMessage.get_actual_run_date(msg)
                    context['ti'].xcom_push(key='_message_value', value=run_date)
                    ack_ids = [m['ackId'] for m in pulled_message if m.get('ackId')]
                    hook.acknowledge(self.project, self.subscription, ack_ids)
                    print("The Pulled message is acknowledged")
                else:
                    ack_ids = [m['ackId'] for m in pulled_message if m.get('ackId')]
                    hook.acknowledge(self.project, self.subscription, ack_ids)
                    print("Acknowledged the irrelevant message")
            return bool(message_result)
        else:
            print("No message to read at the moment! Sensor moves to sleep mode.")
