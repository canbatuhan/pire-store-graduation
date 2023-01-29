import socket
from typing import Tuple

from pire.util.constants import BUFFER_SIZE, ENCODING
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException
from pire.util.logger import Logger


class UserHandler:
    def __init__(self, client_id:str, host:str, ui_port:int) -> None:
        self.__host = host
        self.__ui_port = ui_port
        self.__ui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__logger = Logger("Communication-Handler-User-Handler", client_id)

    def __throw_exception(self, addr:Tuple[str,int]) -> None:
        self.__logger.failure("Connection with {}:{} is lost.".format(*addr))
        raise ConnectionLostException()

    def start(self) -> None:
        self.__ui_socket.bind((self.__host, self.__ui_port))
        self.__ui_socket.listen()
        self.__logger.info("Listening on {}:{}.".format(self.__host, self.__ui_port))

    def establish_connection(self) -> Tuple[socket.socket,Tuple[str,int]]:
        connection, addr = self.__ui_socket.accept()
        self.__logger.info("TCP connection is established with user {}:{}.".format(*addr))
        return connection, addr

    def receive_request(self, connection:socket.socket, addr:Tuple[str,int]) -> str:
        try: # Try to receive
            user_request = connection.recv(BUFFER_SIZE).decode(ENCODING)
            self.__logger.info("Request  '{}' received from {}:{}.".format(user_request, *addr))
        except: # Fail to receive
            self.__throw_exception(addr)
        return user_request

    def parse_request(self, request:str) -> Tuple[Events,str,object]:
        return None, None, None

    def send_ack(self, connection:socket.socket, addr:Tuple[str,int], ack:str) -> None:
        try: # Try to send
            connection.send(ack.encode(ENCODING))
            self.__logger.info("{}:{} is acknowledged '{}'".format(*addr, ack))
        except: # Fail to send
            self.__throw_exception(addr)
    
    def close_connection(self, connection:socket.socket, addr:Tuple[str,int]) -> None:
        try: # Try to close
            connection.close()
            self.__logger.info("TCP connection is closed with user {}:{}.".format(*addr))
        except: # Fail to close, already lost
            self.__throw_exception(addr)
