"""Message Broker"""
import logging
import enum
import socket
from typing import Dict, List, Any, Tuple

logging.basicConfig(filename="log.log", level=logging.DEBUG)

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        print("Broker initialized")
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.topics = {}
        self.lastmsg = {}
        self.subscriptions = {}
        self.sock = socket.socket()
        self.sock.bind((self._host, self._port))


            
        

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.lastmsg.keys())


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.lastmsg[topic]


    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.lastmsg[topic] = value


    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return self.subscriptions[topic]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if (address, _format) not in self.subscriptions[topic]:
            logging.debug("Subscribed %s to %s", address, topic)
            self.subscriptions[topic].append((address, _format))
        else:
            logging.debug("Already subscribed %s to %s", address, topic)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for sub in self.subscriptions[topic]:
            if sub[0] == address:
                self.subscriptions[topic].remove(sub)
                logging.debug("Unsubscribed %s from %s", address, topic)
                return
        logging.debug("Not subscribed %s to %s", address, topic)

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            pass
