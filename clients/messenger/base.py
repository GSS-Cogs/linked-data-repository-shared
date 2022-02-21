from abc import ABCMeta, abstractmethod


class BaseMessage(metaclass=ABCMeta):
    """
    Generic message class
    """

    def __init__(self, message: object):
        self.message: object = message

    @abstractmethod
    def get_attribute(self, attribute: str):
        ...

    @abstractmethod
    def get(self):
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
