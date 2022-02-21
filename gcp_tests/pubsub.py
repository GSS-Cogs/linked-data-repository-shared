"""
These tests have some degree of reliance on GCP credentials, so will only be
ran on specific user instruction (i.e not by github on PR's).
"""

import logging
import os
from pathlib import Path
from time import sleep
from typing import Union
import uuid


from google.cloud.pubsub_v1 import PublisherClient
from google.pubsub_v1.types import pubsub as pubsub_gapic_types
from google.api_core.exceptions import NotFound
import pytest

from clients.messenger.pubsub import PubSubClient, PubSubMessage
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

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


# Note: We're conciously NOT giving our client the ability to create a topic
# as that's better handled as part of infrastructure.
def create_test_topic() -> pubsub_gapic_types.Topic:
    """
    Helper, creates then returns a test topic
    """
    publisher_client = PublisherClient()
    topic_id = f"test_topic_{str(uuid.uuid4())}"
    topic_path = publisher_client.topic_path(GCP_PROJECT_ID, topic_id)
    topic = publisher_client.create_topic(request={"name": topic_path})
    test_topics.append(topic)
    return topic


# Note: We're conciously NOT giving our client the abilty to create a subscription
# as that's better handled as part of infrastructure.
def create_test_subscription(
    topic: pubsub_gapic_types.Topic,
) -> pubsub_gapic_types.Subscription:
    assert isinstance(topic, pubsub_gapic_types.Topic)
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
        topic = create_test_topic()
        client.put_one_message(topic, "foo message bar")

    def test_message_can_be_published_subscribed_and_read(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out
        """
        topic: pubsub_gapic_types.Topic = create_test_topic()
        subscription: pubsub_gapic_types.Subscription = create_test_subscription(topic)

        msg_to_send = "foo message bar"
        client.put_one_message(topic, msg_to_send)

        client.subscribe(subscription)
        message: PubSubMessage = client.get_next_message()

        assert (
            message.get("content") == msg_to_send
        ), f"Got message {message}, expected message {msg_to_send}"

    def test_unacknowlaged_message_can_be_read_again(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out
        """
        topic: pubsub_gapic_types.Topic = create_test_topic()
        subscription: pubsub_gapic_types.Subscription = create_test_subscription(topic)

        msg_to_send = "foo message bar"
        client.put_one_message(topic, msg_to_send)

        client.subscribe(subscription)

        for _ in range(3):

            message: PubSubMessage = client.get_next_message()

            assert (
                message.get("content") == msg_to_send
            ), f"Got message {message}, expected message {msg_to_send}"

    def test_acknowlaged_message_can_only_be_read_once(self, client: PubSubClient):
        """
        Create topic, write a message to it, subscribe, read it out, the acknowlage it,
        then confirm the topic is now empty
        """
        topic: pubsub_gapic_types.Topic = create_test_topic()
        subscription: pubsub_gapic_types.Subscription = create_test_subscription(topic)

        msg_to_send = "foo message bar"
        client.put_one_message(topic, msg_to_send)

        client.subscribe(subscription)

        message: Union[PubSubMessage, None] = client.get_next_message()
        assert (
            message.get("content") == msg_to_send
        ), f"Got message {message}, expected message {msg_to_send}"
        client.confirm_received(message)

        message2: Union[PubSubMessage, None] = client.get_next_message()
        assert message is not message2

    def test_message_retention_is_configurable(selft):
        """
        Confirm that should we configure 0 retention of known messages, no
        awareness of messages is retained by the client, even where receipt
        has been confirmed
        """

        remove_test_db_if_exists()
        client: PubSubClient = PubSubClient(dbname="test_db.json", message_retention={})

        topic: pubsub_gapic_types.Topic = create_test_topic()
        subscription: pubsub_gapic_types.Subscription = create_test_subscription(topic)

        msg_to_send = "foo message bar"
        client.put_one_message(topic, msg_to_send)

        client.subscribe(subscription)

        for _ in range(3):

            message: PubSubMessage = client.get_next_message()
            client.confirm_received(message)
            assert len(client.deduplicator.db) == 0