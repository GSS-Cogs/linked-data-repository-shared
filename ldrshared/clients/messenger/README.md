
# Messenger Clients

**All** messenger clients are expected to conform to the `BaseMessenger` class as defined in `base.py` and to return python objects conforming to the `BaseMessage` class  (also from `base.py`). 


## GCP PubSub Client: Usage

```python
from typing import Union

from ldrshared.clients import PubSubClient, BaseMessage, BaseMessenger

client: BaseMessenger = PubSubClient()

# -----
# Subscribe and get messages

client.subscribe('subscription-1-to-topic-1')
while True:
    # note: default timeout is 10 seconds, that's probably fine
    message: Union[BaseMessage, None] = client.get_next_message(timeout=30)
    if message:
        # Message contents can be str or dict
        message_content: Union[str, dict] = message.get()

        # If its dict you can get fields directly
        message_content: str = message.get('field', 'OPTIONAL-default-is-None')

        # do something with message_content

# -----
# Put a message
client.put_one_message('topic-1', 'I r a message')
# or
client.put_one_message('topic-1', {'I r': 'a more complex message'})
```

NOTE: You'll also need to:

* Set an environment variable of `GCP_PROJECT_ID` setting your project ID.
* Have credentials, either via service account (for production) or via the `gcloud` cli for development (see the following section on running pubusb tests for details).


## GCP PubSub Client: Running Tests

At time of writing the tests PubSub client tests are running against topics and subscriptions on GCP itself so we're **not** running them on commit, a developer needs to explicitly call them.

These tests **will** clean up after themselves regardless of test results (provided you don't bomb out early - please don't do that).

To run these tests:

* Install the gcp cli https://cloud.google.com/sdk/docs/install
* Authenticate your machine with `gcloud auth login` and your **gsscogs.uk** address
* use `gcloud auth application-default login` and your **gsscogs.uk** address - this will tell gcloud to use your developer credentials for applications locally (in place of holdings lots of different service account credentials on your machine).
* `poetry install`
* `poetry run poetry run pytest -v` 
