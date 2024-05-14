"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from time import sleep
from typing import Any
import socket
from .protocol import PubSub
import selectors


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.format = 0
        self.topic = topic
        self._type = _type
        self.host = 'localhost'
        self.port = 5000
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.pull)


    def push(self, value):
        """Sends data to broker."""
        protocolMessage = PubSub.publish(value, self.topic)
        PubSub.send_msg(self.sock, protocolMessage, self.format)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        protocolMessage = PubSub.recv_msg(self.sock)
        if protocolMessage is None: 
            sleep(0.1)
            self.pull()
        
        return (protocolMessage.topic, protocolMessage.message)


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        protocolMessage = PubSub.list()
        PubSub.send_msg(self.socket, protocolMessage, self.format)

    def cancel(self):
        """Cancel subscription."""
        protocolMessage = PubSub.cancel(self.topic)
        PubSub.send_msg(self.sock, protocolMessage, self.format)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        # print("JSONQueue")
        self.format = 0
        # print("format: ",self.format)
        ## PubSub.send_msg(self.sock, PubSub.serialize(self.format), 0)
        if _type == MiddlewareType.CONSUMER:
            PubSub.send_msg(self.sock, PubSub.subscribe(topic, self.format), self.format)

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        # print("XMLQueue")
        self.format = 1
        # print("format: ",self.format)
        ## PubSub.send_msg(self.sock, PubSub.serialize(self.format), 1)
        if _type == MiddlewareType.CONSUMER:
            PubSub.send_msg(self.sock, PubSub.subscribe(topic, self.format), self.format)

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        # print("PickleQueue")
        self.format = 2
        # print("format: ",self.format)
        ## PubSub.send_msg(self.sock, PubSub.serialize(self.format), 2)
        if _type == MiddlewareType.CONSUMER:
            PubSub.send_msg(self.sock, PubSub.subscribe(topic, self.format), self.format)
