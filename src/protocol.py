"""Protocol for chat server - Computação Distribuida Assignment 3."""
import json
import xml.etree.ElementTree as xml
import pickle
from socket import socket

JSON = 0
XML = 1
PICKLE = 2

"""
    root = ET.fromstring(xmlstring)
        root.tag
        root.attrib
        ...
"""


class Message:
    """Message Type."""
    def __init__(self, command) -> None:
        """Initializes message"""
        self.command = command
    
class SubscribeMessage(Message):
    """Message to Subscribe a chat topic."""
    def __init__(self, topic: str) -> None:
        """Initializes message"""
        super().__init__("subscribe")
        self.topic = topic

    def __repr__(self) -> str:
        data = {"command": "subscribe", "topic": self.topic}
        return json.dumps(data)
    
class PublishMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, message: str, topic: str) -> None:
        """Initializes message"""
        super().__init__("publish")
        self.message = message
        self.topic = topic

    def __repr__(self) -> str:
        data = {"command": "publish", "message": self.message, "topic": self.topic}
        return json.dumps(data)

class ListMessage(Message):
    """Message to list all chat topics."""
    def __init__(self) -> None:
        """Initializes message"""
        super().__init__("list")
    
    def __repr__(self) -> str:
        data = {"command": "list"}
        return json.dumps(data)

class CancelMessage(Message):
    """Message to cancel a chat topic subscription."""
    def __init__(self, topic: str = None) -> None:
        """Initializes message"""
        super().__init__("cancel")
        self.topic = topic
    
    def __repr__(self) -> str:
        data = {"command": "cancel", "topic": self.topic}
        return json.dumps(data)


class PubSub:
    """Computação Distribuida Protocol."""
    
    @classmethod
    def subscribe(cls, topic: str) -> Message:
        """Subscribe to a chat topic."""
        return SubscribeMessage(topic)
    
    @classmethod
    def publish(cls, message: str, topic: str = None) -> Message:
        """Publish a message to a chat topic."""
        return PublishMessage(message, topic)
    
    @classmethod
    def list(cls) -> Message:
        """List all chat topics."""
        return ListMessage()
    
    @classmethod
    def cancel(cls) -> Message:
        """Cancel a chat topic subscription."""
        return CancelMessage()
   
    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        message = json.dumps(msg.__dict__).encode("utf-8")
        size = len(message)
        zero = b'\x00'
        header = size.to_bytes(2, "big")
        completeMessage = zero + header + message
        connection.sendall(completeMessage)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        format = int.from_bytes(connection.recv(1), 'big') 
        print("format:", format, ": format")
        if format == JSON:
            """ message in json format """
            message = json.loads(connection.recv(1024).decode("utf-8"))
        elif format == XML:
            """ message in xml format """
            message = xml.fromstring(connection.recv(1024).decode("utf-8"))
        elif format == PICKLE:
            """ message in pickle format """
            message = pickle.loads(connection.recv(1024).decode("utf-8"))   
            print("wtf")
            pass
        else:
            print("format: ", format)
            pass
        

        if message["command"] == "subscribe":    
            return PubSub.subscribe(message["topic"])
        elif message["command"] == "publish":
            return PubSub.publish(message["message"], message["topic"])
        elif message["command"] == "list":
            return PubSub.list()
        elif message["command"] == "cancel":
            return PubSub.cancel(message["topic"])
        
 

class PubSubBadFormat(Exception):
    """Exception when source message is not PubSub."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
