"""Protocol for chat server - Computação Distribuida Assignment 3."""
import json
import xml.etree.ElementTree as xml
import pickle
from datetime import datetime
from socket import socket

JSON = 0
XML = 1
PICKLE = 2


class Message:
    """Message Type."""
    def __init__(self, command) -> None:
        """Initializes message"""
        self.command = command
    
class SubscribeMessage(Message):
    """Message to Subscribe a chat topic."""
    
class PublishMessage(Message):
    """Message to chat with other clients."""

class ListMessage(Message):
    """Message to list all chat topics."""

class CancelMessage(Message):
    """Message to cancel a chat topic subscription."""


class PubSub:
    """Computação Distribuida Protocol."""
    
    @classmethod
    def subscribe(cls, topic: str) -> Message:
        """Subscribe to a chat topic."""
        print("subscrito (confia)")
        pass
    
    @classmethod
    def publish(cls, message: str, topic: str = None) -> Message:
        """Publish a message to a chat topic."""
        print("aquilo enviou (confia)")
        pass
    
    @classmethod
    def list(cls) -> Message:
        """List all chat topics."""
        print("confia que não há tópicos")
        pass
    
    @classmethod
    def cancel(cls) -> Message:
        """Cancel a chat topic subscription."""
        print("cancelado (confia)")
        pass
   
    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        message = json.dumps(msg.__dict__).encode("utf-8")
        size = len(message)
        header = size.to_bytes(2, "big")
        completeMessage = header + message
        connection.sendall(completeMessage)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        format = connection.recv(1).decode("utf-8") 
        if format == JSON:
            """ message in json format """
            message = json.loads(connection.recv(1024))
        elif format == XML:
            """ message in xml format """
            message = xml.fromstring(connection.recv(1024))
        elif format == PICKLE:
            """ message in pickle format """
            message = pickle.loads(connection.recv(1024))
        else:
            raise ValueError("Invalid format")
        
        if message["command"] == "register":
            return RegisterMessage(message["user"])
        if message["command"] == "subscribe":    
            return SubscribeMessage(message["topic"])
        if message["command"] == "message":   
            return TextMessage(message["message"], message.get("topic", None), message["ts"])
        

        

        """
        
        try:
            header = connection.recv(2)
            n = int.from_bytes(header,byteorder="big")
            data = connection.recv(n)
            dic = json.loads(data)
            if dic["command"] == "register":
                return cls.register(dic["user"])
            if dic["command"] == "Subscribe":
                return cls.Subscribe(dic["topic"])
            if dic["command"] == "message":
                return cls.message(dic["message"], dic.get("topic", None))
        except json.JSONDecodeError:
            
            raise PubSubBadFormat(data)
        
        """
 

class PubSubBadFormat(Exception):
    """Exception when source message is not PubSub."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
