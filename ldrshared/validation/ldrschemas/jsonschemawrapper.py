from jsonschema import validate, ValidationError

from typing import Optional


def confirm_indexer_trigger_message(message_dict: dict) -> Optional[ValidationError]:
    """
    Validates dict against required schema of the message used to trigger the
    linked-data-repository-indexer service
    """

    # Note: At a time of writing the expected behaviour is the message
    # will point to an input file (a csvw) on an attached volume, hence
    # "input_data_path", change as needed as our thinking develops.
    
    # IMPORTANT: if you do change this, make sure to flush the queue,
    # changing what is and is not a valid message while there are messages
    # in flight is probably to be avoided.
    
    schema = {
        "additionalProperties": False,
        "properties": {
            "input_data_path": {"type": "string"},
        },
        "required": ["input_data_path"],
    }
    validate(instance=message_dict, schema=schema)
