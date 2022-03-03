# Simple Validation Helpers

## ldrSchemas

Json/dictionary schemas enforced across the linked data repository apps. This allows us to police from a central location.

Example use case - lets us confirm messages are _not_ malformed and _do_ match the expected schema at the point they both enter and exit the queue, this allows us to shield the app logic from bad assumptions.

Example usage (with PubDSub client):

```python
from ldrshared.client import PubSubClient
from ldrshared.validation import ldrschemas, ValidationError

message: Optional[BaseMessage] = client.get_next_message()

my_message_as_dict: dict = message.get()
schema_version: Optional[int] = int(message.get_attribute("_version", None))
if None:
    raise ValidationError('')

try:
    ldrschemas.confirm_indexer_trigger_message(1, my_message_as_dict)
except ValidationError:
    # It's malformed, do something about that.
```