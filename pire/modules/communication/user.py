import socket
import re
from typing import Tuple

from pire.util.constants import BUFFER_SIZE, ENCODING
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException, InvalidRequestType
from pire.util.logger import Logger


class UserHandler:
    def __init__(self, host:str, ui_port:int) -> None:
        self.__host = host
        self.__ui_port = ui_port
        self.__ui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__logger = Logger("Communication-Handler-User-Handler")

    def start(self) -> None:
        self.__ui_socket.bind((self.__host, self.__ui_port))
        self.__ui_socket.listen()
        self.__logger.info("Listening on {}:{}.".format(self.__host, self.__ui_port))

    def establish_connection(self) -> Tuple[socket.socket,Tuple[str,int]]:
        connection, addr = self.__ui_socket.accept()
        self.__logger.info("TCP connection is established with user {}:{}.".format(*addr))
        return connection, addr

    def receive_request(self, connection:socket.socket, addr:Tuple[str,int]) -> bytes:
        try: # Try to receive
            user_request = connection.recv(BUFFER_SIZE)
            self.__logger.info("Request  '{}' received from {}:{}.".format(user_request, *addr))
        except: # Fail to receive
            self.__logger.failure("Connection with {}:{} is lost.".format(*addr))
            raise ConnectionLostException()
        return user_request

    def parse_request(self, request:bytes) -> Tuple[Events,bytes,bytes]:
        request_as_str = request.decode(ENCODING)
        create_pattern = re.compile(r'create\((\w+),(\d+)\)')
        read_pattern = re.compile(r'read\((\w+)\)')
        update_pattern = re.compile(r'update\((\w+),(\d+)\)')
        delete_pattern = re.compile(r'delete\((\w+)\)')

        if re.match(create_pattern, request_as_str):
            match = re.match(create_pattern, request_as_str)
            return (Events.CREATE,
                    match.group(1).encode(ENCODING),
                    match.group(2).encode(ENCODING))

        elif re.match(read_pattern, request_as_str):
            match = re.match(read_pattern, request_as_str)
            return (Events.READ,
                    match.group(1).encode(ENCODING),
                    None)

        elif re.match(update_pattern, request_as_str):
            match = re.match(update_pattern, request_as_str)
            return (Events.UPDATE,
                    match.group(1).encode(ENCODING),
                    match.group(2).encode(ENCODING))

        elif re.match(delete_pattern, request_as_str):
            match = re.match(delete_pattern, request_as_str)
            return (Events.DELETE,
                    match.group(1).encode(ENCODING),
                    None)

        else: # User entered invalid request
            raise InvalidRequestType()

    def send_ack(self, connection:socket.socket, addr:Tuple[str,int], ack:bool, value:bytes) -> None:
        try: # Try to send
            if ack: ack_msg = "Succes, value={}\n".format(value)
            else: ack_msg = "Failure\n"
            
            connection.send(ack_msg.encode(ENCODING))
            self.__logger.info("{}:{} is acknowledged '{}'".format(*addr, ack_msg))
            
        except: # Fail to send
            self.__logger.failure("Connection with {}:{} is lost.".format(*addr))
            raise ConnectionLostException()
    
    def close_connection(self, connection:socket.socket, addr:Tuple[str,int]) -> None:
        try: # Try to close
            connection.close()
            self.__logger.info("TCP connection is closed with user {}:{}.".format(*addr))
        except: # Fail to close, already lost
            self.__logger.failure("Connection with {}:{} is lost.".format(*addr))
            raise ConnectionLostException()
