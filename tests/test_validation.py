from socket import MSG_DONTWAIT
import pytest
from unittest.mock import Mock

from ldrshared.constants import MESSAGE_SCHEMA_KEY
from ldrshared.validation import ldrschemas, ValidationError


def test_confirm_indexer_trigger_message_v1_allows_valid():
    """
    Check that confirm_indexer_trigger_message allows a valid message schema
    """
    msg = Mock()
    msg.get_attribute = lambda x, y: {MESSAGE_SCHEMA_KEY: "1"}.get(x, y)
    msg.get = lambda: {"input_data_path": "foo"}
    ldrschemas.confirm_indexer_trigger_message(msg)


def test_confirm_indexer_trigger_message_v1_raises_with_missing_key():
    """
    Check that confirm_indexer_trigger_message raise for a missing
    required key
    """

    msg = Mock()
    msg.get_attribute = lambda x, y: {MESSAGE_SCHEMA_KEY: "1"}.get(x, y)
    msg.get = lambda: {"wrong_key": "foo"}

    with pytest.raises(ValidationError) as excinfo:
        ldrschemas.confirm_indexer_trigger_message(msg)
    assert "'wrong_key' was unexpected" in str(excinfo.value)


def test_confirm_indexer_trigger_message_v1_raises_with_too_many_keys():
    """
    Check that confirm_indexer_trigger_message raises for additional
    unexpected message keys
    """

    msg = Mock()
    msg.get_attribute = lambda x, y: {MESSAGE_SCHEMA_KEY: "1"}.get(x, y)
    msg.get = lambda: {
        "input_data_path": "foo",
        "some_random_thing_someone_added": "bar",
    }
    with pytest.raises(ValidationError) as excinfo:
        ldrschemas.confirm_indexer_trigger_message(msg)
    assert "Additional properties are not allowed" in str(excinfo.value)


def test_message_schema_does_not_exist_raises():
    """
    Check that the appropriate error is raised where a dict schema
    does not exist
    """

    msg = Mock()
    msg.get_attribute = lambda x, y: {
        MESSAGE_SCHEMA_KEY: "__not_a_schema_version__"
    }.get(x, y)

    def raise_(ex):
        raise ex

    msg.get = lambda: raise_(Exception("Test should never get this far"))

    with pytest.raises(ValidationError) as excinfo:
        ldrschemas.confirm_indexer_trigger_message(msg)
    assert "is not in known versions" in str(excinfo.value)
