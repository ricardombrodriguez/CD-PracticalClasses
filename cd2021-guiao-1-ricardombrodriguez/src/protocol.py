"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""

    def __init__(self,command):
        self.command = command


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self,command,channel):
        super().__init__(command)
        self.channel = channel

    def __str__(self):
        return f'{{"command": "join", "channel": "{self.channel}"}}'


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self,command,username):
        super().__init__(command)
        self.username = username

    def __str__(self):
            return f'{{"command": "register", "user": "{self.username}"}}'

    
class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self,command,message,ts,channel=None):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = ts

    def __str__(self):
        return f'{{"command": "message", "message": "{self.message}", "ts": {self.ts}}}'


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage"""

        return RegisterMessage("Register",username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""

        return JoinMessage("Join",channel)


    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""

        ts = int(datetime.now().timestamp())            #get current timestamp
        return TextMessage("Text",message,ts,channel)


    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""

        if (type(msg) == RegisterMessage):         #RegisterMessage as JSON                 
            jsonStr = json.dumps({"command": "register", "user": msg.username }).encode("UTF-8")      
        elif (type(msg) == JoinMessage):           #JoinMessage as JSON   
            jsonStr = json.dumps({"command": "join", "channel": msg.channel }).encode("UTF-8")
        elif (type(msg) == TextMessage):           #TextMessage as JSON    
            jsonStr = json.dumps({"command": "message", "message": msg.message, "channel": msg.channel, "ts": msg.ts}).encode("UTF-8") 
        header = len(jsonStr).to_bytes(2, "big")    
        connection.sendall(header + jsonStr)
        

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        
        headerBytes = connection.recv(2)                            #receive first 2 bytes
        if headerBytes:                                             #if there is an header (e.g. a message)

            header = int.from_bytes(headerBytes,"big")              #store message length
            try :                                               
                received = connection.recv(header).decode("UTF-8")  #decodes the connection recv message
                data = json.loads(received)                         #transforms "received" in a kind of JSON dictionary
            except json.JSONDecodeError as error:
                raise CDProtoBadFormat(received)

            if (data["command"] == "register"):                     #create a RegisterMessage object
                username = data["user"]
                return CDProto.register(username)

            elif (data["command"] == "join"):                       #create a JoinMessage object
                channel = data["channel"]
                return CDProto.join(channel)

            elif (data["command"] == "message"):                    #create a TextMessage object
                msg = data["message"]
                if ("channel" in data and data["channel"]):
                    channel = data["channel"]
                    return CDProto.message(msg,channel)
                else:  
                    return CDProto.message(msg)
 

class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")