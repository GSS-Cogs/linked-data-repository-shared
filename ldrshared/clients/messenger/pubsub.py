from concurrent.futures._base import TimeoutError
from dataclasses import dataclass
import datetime
import logging
import json
import os
import time
from typing import Union, List, Optional

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.publisher.futures import Future
from google.pubsub_v1.types import pubsub as pubsub_gapic_types
from google.cloud.pubsub_v1.subscriber.message import Message
from tinydb import TinyDB, Query

from .base import BaseMessenger, BaseMessage
from ldrshared.constants import MESSAGE_SCHEMA_KEY

STR_TIME = "%b/%d/%Y %H:%M:%S"
TIME_STAMP_KEY = "_published_time_stamp"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", None)
if not GCP_PROJECT_ID:
    raise ValueError("You need to set the project id via the env var GCP_PROJECT_ID")


# Note: docstrings inherit from base class
@dataclass
class PubSubMessage(BaseMessage[Message]):
    message: Message

    def get_attribute(self, key: Optional[str], default: Optional[str] = None):
        return self.message.attributes.get(key, default)

    def get(
        self, key: Optional[str] = None, default: Optional[str] = None
    ) -> Union[dict, str, list]:
        data: str = self.message.data.decode("utf-8")
        if key:
            data_as_dict: dict = json.loads(data)
            return data_as_dict.get(key, default)

        try:
            return json.loads(data)
        except:
            return data


# TODO - will need to be implemented with an actual database
# before we start subscribing from more than one location
class Deduplicator:
    """
    Uses simple database operations to check if a given message has been
    received before.
    """

    def __init__(self, dbname: str, message_retention: dict):
        self.db = TinyDB(dbname)

        # Confirm our dict only contains what it should.
        assert all(
            [x in ["days", "hours", "minutes", "seconds"] for x in message_retention]
        )
        assert all([isinstance(x, int) for x in message_retention.values()])

        self.message_retention: datetime.timedelta = datetime.timedelta(
            days=message_retention["days"] if "days" in message_retention else 0,
            hours=message_retention["hours"] if "hours" in message_retention else 0,
            minutes=message_retention["minutes"]
            if "minutes" in message_retention
            else 0,
            seconds=message_retention["seconds"]
            if "seconds" in message_retention
            else 0,
        )

    @staticmethod
    def _message_to_dict(message: Message) -> dict:
        """
        Convert type pubsub_v1.subscriber.message.Message to a simple dict
        """
        return {
            "content": message.data.decode("utf-8"),
            "time_stamp": message.attributes.get(TIME_STAMP_KEY),
        }

    def store(self, message: Message):
        """
        Stores the content and timestamp of a single message
        """
        message_as_dict: dict = self._message_to_dict(message)
        if message_as_dict["time_stamp"]:
            self.db.insert(message_as_dict)
            self._housekeeping()
        else:
            # Malformed time stamp, ignore but log it
            logging.warning(
                'Unstructured message passed to store: %s, "time_stamp" value was %s',
                message_as_dict,
                message.attributes.get(TIME_STAMP_KEY),
            )

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

    def message_is_in_list(
        self, message: Message, list_of_messages: List[Message]
    ) -> bool:
        """
        Does the received message already exist in the provided list of messages.

        Note, we need to cast the message to a much simplified dict form as pubsub.Message
        attributes otherwise render all messages unique, even where they are duplicates
        in term of being the same pushed message.
        """
        new_message = self._message_to_dict(message)
        known_messages = [self._message_to_dict(x) for x in list_of_messages]

        return new_message in known_messages

    def _housekeeping(self):
        """
        Simple mechanism for stopping the inline database from getting
        too bloated by dropping records that are older than:

        datetime.now() - self.message_retention # a datetime.timedelta()
        """
        self.db: TinyDB

        # Note: .doc_id's are auto incrementing.
        record_id = min([int(x.doc_id) for x in self.db])
        passes = 0
        max_passes = len(self.db)

        while True:
            record = self.db.get(doc_id=record_id)
            time_stamp_as_datetime = datetime.datetime.strptime(
                record["time_stamp"], STR_TIME
            )
            if time_stamp_as_datetime <= (
                datetime.datetime.now() - self.message_retention
            ):
                logging.warning(f"Length was {len(self.db)}")
                self.db.remove(doc_ids=[record_id])
                logging.warning(f"Length is {len(self.db)}")
                record_id += 1
            else:
                logging.warning(
                    f"Comparing: {time_stamp_as_datetime} and {datetime.datetime.now() - self.message_retention}"
                )
                break

            # Never look beyond the last populated index
            passes += 1
            if passes == max_passes:
                break


class PubSubClient(BaseMessenger):
    def _setup(self, dbname: str = "db.json", message_retention: dict = {"days": 7}):
        """
        Setup for pubsub client

        dbname: Filename for the Tinydb inline message database,
                configurable for testing.
        message_retention: Allows altering of retention period
                for inline message database, configurable for testing,
        """
        self._project_id = GCP_PROJECT_ID
        self.message_buffer = []
        self.deduplicator = Deduplicator(dbname, message_retention)
        self.found_message_duplicate = False

    def subscribe(self, subscription: str):
        """
        Subscribe the messenger client
        """
        subscriber_client = SubscriberClient()

        subscription_path: str = subscriber_client.subscription_path(
            GCP_PROJECT_ID, subscription
        )

        def callback(message: Message) -> None:
            # Is message in database as previously processed?
            if self.deduplicator.is_new_message(message):
                message.nack()
                # Is message already in buffer waiting to be processed?
                if not self.deduplicator.message_is_in_list(
                    message, self.message_buffer
                ):
                    self.message_buffer.append(message)
            else:
                message.ack()
                self.found_message_duplicate = True

        self.streaming_pull_future: StreamingPullFuture = subscriber_client.subscribe(
            subscription_path, callback=callback
        )

    def get_next_message(self, timeout: int = 2) -> Optional[PubSubMessage]:
        """
        Get the next message from the currently subscribed topic
        """

        # Don't pull if we already have messages in the buffer, but disregard
        # any messages without the time stamp required for deduplication
        if any(self.message_buffer):
            return PubSubMessage(self.message_buffer.pop(0))

        subscriber_client = SubscriberClient()
        streaming_pull_future: StreamingPullFuture = self.streaming_pull_future

        # Go looking (for length of timeout: seconds) for new messages
        with subscriber_client:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                pass

        if any(self.message_buffer):
            return self.get_next_message(timeout=timeout)
        return None

    def put_one_message(
        self,
        topic_label: str,
        message_content: Union[str, dict],
        message_schema: Optional[int] = None,
        **message_attributes,
    ):
        """
        Put one message into the subscribed topic. The message
        must be a str or a dictionary (that will be cast to str).
        """
        if isinstance(message_content, dict):
            message_content = json.dumps(message_content)

        if message_attributes:
            message_attributes[TIME_STAMP_KEY] = datetime.datetime.now().strftime(
                STR_TIME
            )
        else:
            message_attributes = {
                TIME_STAMP_KEY: datetime.datetime.now().strftime(STR_TIME)
            }

        if message_schema:
            message_attributes[MESSAGE_SCHEMA_KEY] = message_schema

        logging.info(
            f"""
        Publishing message with text: {message_content}
        And attributes: {message_attributes}
        """
        )

        publisher_client = PublisherClient()
        message_as_bytes: bytes = message_content.encode("utf-8")
        publish_future: Future = publisher_client.publish(
            topic_label, message_as_bytes, **message_attributes
        )
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
