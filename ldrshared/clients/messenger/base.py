from abc import ABCMeta, abstractmethod
from typing import Union


class BaseMessage(metaclass=ABCMeta):
    """
    Generic message class
    """

    def __init__(self, message: object):
        self.message: object = message

    @abstractmethod
    def get_attribute(self, key: str, default=None) -> str:
        """
        Get a single attribute as attached to the message.

        Follows dictionary get pattern, i.e: .get(key, default)
        """
        ...

    @abstractmethod
    def get(self, key=None, default=None) -> Union[str, dict]:
        """
        Get message as either dict or string. Message structure is:

        {
            "time_stamp": "<INSTANT OF PUBLICATION PUT>",
            "content": "<THE 'MESSAGE' THAT WAS PASSED IN>"
        }

        Empty parenthesis will return the whole message as dict,
        you can also .get(<TOP_LEVEL_KEY>) directly if you want to.

        Returns will be type dict where castable else str.
        """
        ...


class BaseMessenger(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        self._setup(*args, **kwargs)

    @abstractmethod
    def _setup(self, *args, **kwargs):
        """
        Generic setup logic to be called when the
        class is instantiated.

        Where possible take variabels from ENV VARS
        rather than args and kwargs to avoid fouling
        your application logic.
        """
        ...

    @abstractmethod
    def subscribe(self, *args, **kwargs):
        """
        Subscribe the messenger client to a topic
        """
        ...

    @abstractmethod
    def get_next_message(self) -> BaseMessage:
        """
        Get the next message from the currently subscribed to topic
        """
        ...

    @abstractmethod
    def put_one_message(self, message: str):
        """
        Put one message into the subscribed topic
        """
        ...

    @abstractmethod
    def confirm_received(self, message: BaseMessage):
        """
        Given a message, confirm that the message has been
        received.

        If your client does not required receipt, just
        implement with a pass statment.
        """
        ...
