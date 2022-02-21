from concurrent.futures._base import TimeoutError
import datetime
import json
import os
from typing import Union, List

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.publisher.futures import Future
from google.pubsub_v1.types import pubsub as pubsub_gapic_types
from google.cloud.pubsub_v1.subscriber.message import Message
from tinydb import TinyDB, Query, where

from .base import BaseMessenger, BaseMessage

STR_TIME = "%b/%d/%Y %H:%M:%S"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", None)
if not GCP_PROJECT_ID:
    raise ValueError("You need to set the project id via the env var GCP_PROJECT_ID")


class PubSubMessage(BaseMessage):
    message: Message

    def get_attribute(self, attribute: str):
        """
        Gets contents of a single message attribute
        """
        return getattr(self.message, attribute)

    def get(self, key=None, default_value=None) -> Union[dict, str]:
        """
        Gets message content. Empty parenthesis returns the whole
        message dict. Though you can also pass through a dictionary
        style get, i.e .get(key, default)
        """
        data_dict: dict = json.loads(self.message.data.decode("utf-8"))
        if not key:
            return data_dict
        else:
            return data_dict.get(key, default_value)


# TODO - will need to be implemented with an actual database
# before we start subscribing from more than one location
class Deduplicator:
    """
    Uses simple database operations to check if a given message has been
    received before.
    """

    def __init__(self, dbname: str, message_retention: dict):
        self.db = TinyDB(dbname)
        self.message_retention: datetime.timedelta = datetime.timedelta(
            days = message_retention["days"] if "days" in message_retention else 0,
            hours = message_retention["hours"] if "hours" in message_retention else 0,
            minutes = message_retention["minutes"] if "minutes" in message_retention else 0,
            seconds = message_retention["seconds"] if "seconds" in message_retention else 0
        )

    @staticmethod
    def _message_to_dict(message: Message) -> dict:
        data_dict = json.loads(message.data.decode("utf-8"))
        return {"content": data_dict["content"], "time_stamp": data_dict["time_stamp"]}

    def store(self, message: Message):
        """
        Stores the content and timestamp of a single message
        """
        self.db.insert(self._message_to_dict(message))
        self._housekeeping()

    def is_new_message(self, message: Message) -> bool:
        """
        Check if the message received is a new message
        """
        new_message_as_dict = self._message_to_dict(message)
        q = Query()
        found = self.db.search(
            (q.content == new_message_as_dict["content"])
            & (q.time_stamp == new_message_as_dict["time_stamp"])
        )

        return len(found) == 0

    def message_is_in_list(self, message: Message, list_of_messages: List[Message]) -> bool:
        """
        Does the received message already exist in the provided list of messages.

        Note, we need to cast the message to a much simplified form as pubsub.Message
        attributes otherwise render all messages unique, even where they are duplicates.
        """
        new_message = self._message_to_dict(message)
        known_messages = [self._message_to_dict(x) for x in list_of_messages]
        return new_message in known_messages

    def _housekeeping(self):
        """
        Simple mechanism for stopping the inline database from getting 
        too bloated by dropping records that are older than:
        
        dateime.now() - self.message_retention # a datetime.timedelta()
        """

        # Note: .doc_id's are auto incrementing.
        record_id = min([int(x.doc_id) for x in self.db])
        passes = 0
        max_passes = len(self.db)

        while True:
            record = self.db.get(doc_id=record_id)
            time_stamp_as_datetime = datetime.datetime.strptime(record["time_stamp"], STR_TIME)
            if time_stamp_as_datetime < (datetime.datetime.now() - self.message_retention):
                self.db.remove(where('_default') == record_id)
                record_id = str(int(record_id) + 1)
            else:
                break

            # Never look beyond the last populated index
            passes += 1
            if passes == max_passes:
                break


class PubSubClient(BaseMessenger):
    def _setup(self, dbname="db.json", message_retention={"days": 7}):
        """
        Setup for pubsub client

        dbname: Filename for the tonydb inline message database,
                for testing.
        message_retention: Allows altering of retention period
                for inline message database, for testing,
        """
        self._project_id = GCP_PROJECT_ID
        self.messages = []
        self.deduplicator = Deduplicator(dbname, message_retention)
        self.found_message_duplicate = False

    def subscribe(self, subscription: pubsub_gapic_types.Subscription):
        """
        Subscribe the messenger client
        """
        subscriber_client = SubscriberClient()

        subscription_path: str = subscriber_client.subscription_path(
            GCP_PROJECT_ID, subscription.name.split("/")[-1]
        )

        def callback(message: Message) -> None:
            if self.deduplicator.is_new_message(message):
                message.nack()
                if not self.deduplicator.message_is_in_list(message, self.messages):
                    self.messages.append(message)
            else:
                self.found_message_duplicate = True

        self.streaming_pull_future: StreamingPullFuture = subscriber_client.subscribe(
            subscription_path, callback=callback
        )

    def get_next_message(self, timeout: int = 10) -> Union[PubSubMessage, None]:
        """
        Get the next message from the currently subscribed topic
        """
        # If we've already got unprocessed messages in the buffer then use the
        # next one, don't bother polling for more
        if len(self.messages) > 0:
            return PubSubMessage(self.messages.pop())

        subscriber_client = SubscriberClient()
        streaming_pull_future: StreamingPullFuture = self.streaming_pull_future

        # Go looking (for length of timeout seconds) for new messages
        with subscriber_client:

            try:
                # Note: if you poll without a timout, its infinite and will block
                # even where one or more messages are found
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()

        # Where we have found a duplicate, we're gonna go round again
        # and look for the next message.
        if self.found_message_duplicate:
            self.found_message_duplicate = False
            return self.get_next_message(timeout=timeout)

        if len(self.messages) > 0:
            msg = PubSubMessage(self.messages.pop())
        else:
            msg = None

        return msg

    def put_one_message(
        self,
        topic: pubsub_gapic_types.Topic,
        message_str: str,
    ):
        """
        Put one message into the subscribed topic. The message
        must be a str or castable to a str.

        This str messge will be added to a dictionary along with
        a time_stamp, eg:

        {
            "content": <YOUR MESSAGE>,
            "time_stamp": <A TIME STAMP>
        }
        """
        if not isinstance(message_str, str):
            message_str = str(message_str)

        publisher_client = PublisherClient()
        message_as_bytes: bytes = json.dumps(
            {"time_stamp": datetime.datetime.now().strftime(STR_TIME), "content": message_str}
        ).encode("utf-8")
        publish_future: Future = publisher_client.publish(topic.name, message_as_bytes)
        publish_future.result()

    def confirm_received(self, message: PubSubMessage):
        """
        Given a message, confirm that the message has been
        received.

        If your client does not required receipt, just
        implement with a pass statment.
        """
        message.message.ack()
        self.deduplicator.store(message.message)
