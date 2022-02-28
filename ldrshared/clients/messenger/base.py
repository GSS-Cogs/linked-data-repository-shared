from abc import ABCMeta, ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, TypeVar, Generic

T = TypeVar("T", bound=object)

@dataclass
class BaseMessage(ABC, Generic[T]):
    message: T

    @abstractmethod
    def get_attribute(self, key: str, default: Union[str, None] = None) -> str:
        """
        Get a single attribute as attached to the message.

        Follows dictionary get pattern, i.e: .get(key, default)
        """
        ...

    @abstractmethod
    def get(self, key: Union[str, None] = None, default: Union[str, None] = None) -> Union[str, dict, list]:
        """
        Get message content. Empty parenthesis will return the whole 
        message as dict or str (dict where castable, else str).

        You can also .get(key) as per standard dictionary get
        mechanism, which will return one of str|dict|list
        depending on the document in question. 
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
