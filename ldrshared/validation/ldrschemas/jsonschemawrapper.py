from typing import Optional

from jsonschema import validate, ValidationError

from ldrshared.clients import BaseMessage
from ldrshared.constants import MESSAGE_SCHEMA_KEY


def confirm_indexer_trigger_message(message: BaseMessage):
    """
    Validates that the indexer triggering message is valid and
    conforms to an appropriate schema.
    """
    schemas = {
        "1": {
            "additionalProperties": False,
            "properties": {
                "input_data_path": {"type": "string"},
            },
            "required": ["input_data_path"],
        }
    }
    _validate_message_schema(message, schemas)


def _validate_message_schema(message: BaseMessage, schemas: dict):
    """
    For a given message, this function will:

    - confirm we have an attribute of MESSAGE_SCHEMA_KEY
    - check that this key exists in the known schemas for this dict
    - check that the message in question actually containts a dict
    - confirm that dict conforms to the claimed schema

    Any failures of the above will raise: ValidationError
    """

    message_schema_version: str = message.get_attribute(MESSAGE_SCHEMA_KEY, None)

    if not message_schema_version:
        raise ValidationError(
            f"Cannot validate message {message_as_dict} as required schema version is missing"
        )

    if message_schema_version not in schemas:
        raise ValidationError(
            f'Message "{MESSAGE_SCHEMA_KEY}" specified schema {message_schema_version},'
            f" is not in known versions: {list(schemas.keys())}"
        )

    message_as_dict: dict = message.get()

    if not isinstance(message_as_dict, dict):
        raise ValidationError("Validating dict message that's not a dict!")

    validate(instance=message_as_dict, schema=schemas[message_schema_version])
