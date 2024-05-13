"""Message Broker"""
import logging
import enum
import socket
from typing import Dict, List, Any, Tuple
import selectors
from .protocol import PubSub

logging.basicConfig(filename="broker.log", level=logging.DEBUG)

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
        self.subscriptions = {}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self._host, self._port))
        self.sock.listen(100)

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)

    def accept(self, sock, mask):
        conn, addr = sock.accept() # Should be ready
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        #Regista ações no respetivo ficheiro
        logging.debug(f"Accepted connection from {addr}")

    def read(self, conn, mask):

        try:
            data = PubSub.recv_msg(conn)
            if data:
                command = data.command

                if command == "subscribe":
                    self.subscribe(data.topic, conn)

                elif command == "publish":
                    self.put_topic(data.topic, data.message)

                elif command == "cancel":
                    self.unsubscribe(data.topic, conn)

                elif command == "list_topics":
                    self.list_topics(conn)

                else:
                    print("Unknown command")
        
        except ConnectionResetError:
            print("Connection closed")
            for topic in self.subscriptions:
                for subscriber in self.subscriptions[topic]:
                    if subscriber[0] == conn:
                        self.subscriptions[topic].remove(subscriber)
                        break
            self.sel.unregister(conn)
            conn.close()
            
        

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.topics.keys())


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        print(self.topics)
        if topic not in self.topics:
            return None
        return self.topics[topic]


    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic] = value
        for curTopic in self.topics:
            if curTopic.startswith(topic):
                if curTopic in self.subscriptions:
                    for subscriber in self.subscriptions[curTopic]:
                        PubSub.send_msg(subscriber[0], PubSub.publish(value, curTopic), subscriber[1])


    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        print(self.subscriptions)
        return self.subscriptions[topic]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic not in self.topics:
            self.topics[topic] = None
            self.subscriptions[topic] = [(address, _format)]
        elif topic not in self.subscriptions:
            logging.debug("Subscribed %s to %s", address, topic)
            self.subscriptions[topic] = [(address, _format)]
        elif (address, _format) not in self.subscriptions[topic]:
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
            events = self.sel.select(timeout=None)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
