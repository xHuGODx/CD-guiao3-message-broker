"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
import socket
from .protocol import PubSub


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self._type = _type
        self.host = 'localhost'
        self.port = 5000
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
    

    def push(self, value):
        """Sends data to broker."""
        protocolMessage = PubSub.publish(value, self.topic)
        PubSub.send_msg(self.sock, protocolMessage)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        protocolMessage = PubSub.recv_msg(self.sock)
        if protocolMessage is None: 
            return None
        
        return (protocolMessage.topic, protocolMessage.message)


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        protocolMessage = PubSub.ask_topics()
        PubSub.send_msg(self.socket, protocolMessage)

    def cancel(self):
        """Cancel subscription."""
        protocolMessage = PubSub.cancel(self.topic)
        PubSub.send_msg(self.sock, protocolMessage)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        PubSub.send_msg(self.sock, PubSub.subscribe(topic))
    


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        PubSub.send_msg(self.sock, PubSub.subscribe(topic))


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        PubSub.send_msg(self.sock, PubSub.subscribe(topic))
