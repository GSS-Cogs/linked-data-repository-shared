"""
These tests currently use GCP credentials which we don't want to manually generate
and push into git.

So DO NOT rename with the test_ prefix as we only want these to run when
explicitly called by a developer, not by pytest discovery.
"""


import os
from pathlib import Path
import time
import uuid

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.pubsub_v1.types import pubsub as pubsub_gapic_types
from google.cloud.pubsub_v1.publisher.futures import Future
from google.api_core.exceptions import NotFound
import pytest

from ldrshared.clients.messenger.pubsub import PubSubClient, PubSubMessage

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", None)
if not GCP_PROJECT_ID:
    raise ValueError("You need to set the project id via the env var GCP_PROJECT_ID")

test_topics = []
test_subscriptions = []


def remove_test_db_if_exists():
    """
    Remove lingering test_db where needed
    """
    old_db = Path("test_db.json")
    if old_db.exists():
        os.remove(old_db)


@pytest.fixture
def client() -> PubSubClient:
    remove_test_db_if_exists()
    return PubSubClient(dbname="test_db.json")


# Note: We're consciously NOT giving our client the ability to create a topic
# as that's better handled as part of infrastructure.
def pristine_test_topic() -> pubsub_gapic_types.Topic:
    """
    Helper, creates then returns a test topic
    """
    publisher_client = PublisherClient()
    topic_id = f"test_topic_{str(uuid.uuid4())}"
    topic_path = publisher_client.topic_path(GCP_PROJECT_ID, topic_id)
    topic = publisher_client.create_topic(request={"name": topic_path})
    test_topics.append(topic)
    return topic


# Note: We're consciously NOT giving our client the abilty to create a subscription
# as that's better handled as part of infrastructure.
def pristine_test_subscription(
    topic: pubsub_gapic_types.Topic,
) -> pubsub_gapic_types.Subscription:
    subscription_client = SubscriberClient()
    subscription_id = f"test_subscription_{str(uuid.uuid4())}"
    subscription_path = subscription_client.subscription_path(
        GCP_PROJECT_ID, subscription_id
    )

    with subscription_client:
        subscription: pubsub_gapic_types.Subscription = (
            subscription_client.create_subscription(
                request={"name": subscription_path, "topic": topic.name}
            )
        )
    test_subscriptions.append(subscription)
    return subscription


class TestViaGCP:
    @classmethod
    def teardown_class(cls):
        """
        Tears down all resources created as part of
        running these tests.
        """

        publisher = PublisherClient()
        for topic in test_topics:
            try:
                publisher.delete_topic(request={"topic": topic.name})
            except NotFound:
                # It's fine to have deleted it already
                pass

        subscriber = SubscriberClient()
        for subscription in test_subscriptions:
            try:
                subscriber.delete_subscription(
                    request={"subscription": subscription.name}
                )
            except NotFound:
                # It's fine to have deleted it already
                pass

    def test_a_message_can_be_published(self, client: PubSubClient):
        """ """
        topic = pristine_test_topic()
        client.put_one_message(topic.name, "foo message bar")

    def test_pull_message_from_empty_queue(self, client: PubSubClient):
        """ """
        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        client.subscribe(subscription.name.split("/")[-1])

        for _ in range(10):
            assert not client.get_next_message()

    def test_message_can_be_published_subscribed_and_read(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out
        """
        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"
        client.put_one_message(topic.name, msg_to_send)

        client.subscribe(subscription.name.split("/")[-1])
        message: PubSubMessage = client.get_next_message()

        assert (
            message.get() == msg_to_send
        ), f"Got message {message}, expected message {msg_to_send}"

    def test_unacknowlaged_message_can_be_read_again(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out
        """
        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"
        client.put_one_message(topic.name, msg_to_send)

        client.subscribe(subscription.name.split("/")[-1])

        for _ in range(3):
            message: PubSubMessage = client.get_next_message()

            assert (
                message.get() == msg_to_send
            ), f"Got message {message}, expected message {msg_to_send}"

    def test_acknowlaged_message_can_only_be_read_once(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out, then acknowlage it,
        then confirm we cannot pull the message again.
        """
        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"
        client.put_one_message(topic.name, msg_to_send)

        client.subscribe(subscription.name.split("/")[-1])

        message: PubSubMessage = client.get_next_message()
        assert (
            message.get() == msg_to_send
        ), f"Got message {message}, expected message {msg_to_send}"
        client.confirm_received(message)

        message2: None = client.get_next_message()
        assert not message2

    def test_attributes_can_be_added_to_messages(self, client: PubSubClient):
        """
        Confirm that attributes can be added to messages
        """

        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"
        client.put_one_message(topic.name, msg_to_send, foo="bar")

        client.subscribe(subscription.name.split("/")[-1])

        message: PubSubMessage = client.get_next_message()
        assert message.get_attribute("foo") == "bar"

    def test_dict_message_functionality(self, client: PubSubClient):
        """
        Test basic message as dictionary functionality
        """

        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = {"a_field": "a_value"}
        client.put_one_message(topic.name, msg_to_send)

        client.subscribe(subscription.name.split("/")[-1])

        message: PubSubMessage = client.get_next_message()

        assert isinstance(message.get(), dict)
        assert message.get("a_field") == "a_value"

    def test_message_retention_is_configurable(self):
        """
        Confirm that should we configure 0 retention of recieved messages, no
        awareness of messages is retained by the client, even where receipt
        has been confirmed
        """

        remove_test_db_if_exists()
        client: PubSubClient = PubSubClient(dbname="test_db.json", message_retention={})

        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"
        client.put_one_message(topic.name, msg_to_send)

        client.subscribe(subscription.name.split("/")[-1])

        message1: PubSubMessage = client.get_next_message()
        assert message1
        client.confirm_received(message1)

        message2: PubSubMessage = client.get_next_message()
        assert message2

    def test_old_messages_are_removed(self):
        """
        Confirm that messages older than the configured message retention
        are removed from the inline database by housekeeping.
        """

        remove_test_db_if_exists()
        client: PubSubClient = PubSubClient(
            dbname="test_db.json", message_retention={"seconds": 5}
        )

        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        msg_to_send = "foo message bar"

        # Fill then remove old messages from a pristine inline db
        client.put_one_message(topic.name, msg_to_send)
        client.put_one_message(topic.name, msg_to_send)
        client.put_one_message(topic.name, msg_to_send)
        client.subscribe(subscription.name.split("/")[-1])
        while True:
            msg = client.get_next_message()
            if msg:
                # Note: point at which msg is added to test_db.json
                client.confirm_received(msg)
            else:
                break
        assert len(client.deduplicator.db) > 0
        time.sleep(6)
        client.deduplicator._housekeeping()  # remove messages older than retention
        assert (
            len(client.deduplicator.db) == 0
        ), f"Was pristine database is not empty, has {len(client.deduplicator.db)} records"

        # Fill then remove old messages from a non pristine inline db
        client.put_one_message(topic.name, msg_to_send)
        client.put_one_message(topic.name, msg_to_send)
        client.put_one_message(topic.name, msg_to_send)
        client.subscribe(subscription.name.split("/")[-1])
        while True:
            msg = client.get_next_message()
            if msg:
                # Note: point at which msg is added to test_db.json
                client.confirm_received(msg)
            else:
                break
        assert len(client.deduplicator.db) > 0
        time.sleep(6)
        client.deduplicator._housekeeping()  # remove messages older than retention
        assert (
            len(client.deduplicator.db) == 0
        ), f"Was not pristine database is not empty, has {len(client.deduplicator.db)} records"


    def test_non_client_messages_are_not_stored(self, client: PubSubClient):
        """
        Non client generated messages cannot be used for deduplication as they
        lack the required time_stamp attribute. Comfirm such messages are
        not being stored.
        """

        topic: pubsub_gapic_types.Topic = pristine_test_topic()
        subscription: pubsub_gapic_types.Subscription = pristine_test_subscription(
            topic
        )

        client.subscribe(subscription.name.split("/")[-1])

        publisher_client = PublisherClient()
        message_as_bytes: bytes = 'foooo'.encode("utf-8")
        publish_future: Future = publisher_client.publish(
            topic.name, message_as_bytes
        )
        publish_future.result()

        message1: PubSubMessage = client.get_next_message()
        assert message1
        client.confirm_received(message1)
        assert not any(client.deduplicator.db)




if __name__ == "__main__":
    TestViaGCP()
