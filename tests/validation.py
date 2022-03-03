import pytest

from ldrshared.validation import ldrschemas, ValidationError


def test_confirm_indexer_trigger_message_allows_valid():
    """
    Check that confirm_indexer_trigger_message allows a valid message schema
    """
    valid_message = {"input_data_path": "foo"}
    ldrschemas.confirm_indexer_trigger_message(valid_message)


def test_confirm_indexer_trigger_message_raises_with_missing_key():
    """
    Check that confirm_indexer_trigger_message raise for a missing
    required key
    """

    missing_key_message = {"wrong_key": "foo"}
    with pytest.raises(ValidationError):
        ldrschemas.confirm_indexer_trigger_message(missing_key_message)


def test_confirm_indexer_trigger_message_raises_with_too_many_keys():
    """
    Check that confirm_indexer_trigger_message raises for additional
    unexpected message keys
    """

    excess_fields_message = {
        "input_data_path": "foo",
        "some_random_thing_someone_added": "bar",
    }
    with pytest.raises(ValidationError):
        ldrschemas.confirm_indexer_trigger_message(excess_fields_message)
