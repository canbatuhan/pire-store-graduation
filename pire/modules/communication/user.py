import json
import socket
import re
from typing import Tuple

from pire.util.constants import BUFFER_SIZE, ENCODING
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException, InvalidRequestType


class UserHandler:
    def __init__(self, host:str, ui_port:int) -> None:
        self.__host = host
        self.__ui_port = ui_port
        self.__ui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self) -> None:
        self.__ui_socket.bind((self.__host, self.__ui_port))
        self.__ui_socket.listen()

    def establish_connection(self) -> Tuple[socket.socket,Tuple[str,int]]:
        connection, addr = self.__ui_socket.accept()
        return connection, addr

    def receive_request(self, connection:socket.socket, addr:Tuple[str,int]) -> bytes:
        try: # Try to receive
            user_request = connection.recv(BUFFER_SIZE)
        except: # Fail to receive
            raise ConnectionLostException()
        return user_request

    def parse_request(self, request:bytes) -> Tuple[Events,bytes,bytes]:
        request_as_dict = dict(json.loads(request))

        if request_as_dict.get("command") == Events.CREATE.value:
            return (Events.CREATE,
                    request_as_dict.get("key").encode(ENCODING),
                    request_as_dict.get("value").encode(ENCODING))
        
        elif request_as_dict.get("command") == Events.READ.value:
            return (Events.READ,
                    request_as_dict.get("key").encode(ENCODING),
                    None)
        
        elif request_as_dict.get("command") == Events.UPDATE.value:
            return (Events.UPDATE,
                    request_as_dict.get("key").encode(ENCODING),
                    request_as_dict.get("value").encode(ENCODING))
        
        elif request_as_dict.get("command") == Events.DELETE.value:
            return (Events.DELETE,
                    request_as_dict.get("key").encode(ENCODING),
                    None)

        else: # User entered invalid request
            raise InvalidRequestType()

    def send_ack(self, connection:socket.socket, addr:Tuple[str,int], ack:bool, value:bytes) -> None:
        try: # Try to send
            if ack: ack_msg = json.dumps({"success": True, "value": value})
            else: ack_msg = json.dumps({"success": False, "value": value})
            
            connection.send(ack_msg.encode(ENCODING))
            
        except: # Fail to send
            raise ConnectionLostException()
    
    def close_connection(self, connection:socket.socket, addr:Tuple[str,int]) -> None:
        try: # Try to close
            connection.close()
        except: # Fail to close, already lost
            raise ConnectionLostException()
