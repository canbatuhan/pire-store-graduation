import grpc
import socket
import json
from typing import Iterable, Tuple, List

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.util.logger import Logger
from pire.util.constants import BUFFER_SIZE, ENCODING


class ClusterHandler:
    def __init__(self, host:str, grpc_port:int, neighbours_addr:Iterable[Tuple[str,int]]) -> None:
        self.__host = host
        self.__grpc_port = grpc_port
        self.__neighbours_addr = neighbours_addr
        self.__neighbours = dict()
        self.__logger = Logger("Communication-Handler-Cluster-Handler")

    def __send_greetings(self) -> None:
        for addr, channel in self.__neighbours.items():
            try: # Try to send message
                greeting_stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
                response = greeting_stub.Greet(pirestore_pb2.Greeting(
                    source=pirestore_pb2.Address(host=self.__host, port=self.__grpc_port),
                    destination=pirestore_pb2.Address(host=addr[0], port=addr[1])))

                if response.success:
                    self.__logger.info("Greeted with {}:{}.".format(*addr))
                
                else: # Unable to greet
                    self.__logger.failure("Could not greeted with {}:{}.".format(*addr))
            
            except: # Channel is broken
                self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*addr))

    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:channel})
        self.__send_greetings()


class UserRequestHandler:
    def __init__(self, host, ui_port) -> None:
        self.__host = host
        self.__ui_port = ui_port
        self.__ui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__logger = Logger("Communication-Handler-User-Request-Handler")

    def start(self) -> None:
        self.__ui_socket.bind((self.__host, self.__ui_port))
        self.__ui_socket.listen()
        self.__logger.info("Listening on {}:{}.".format(self.__host, self.__ui_port))

    def __establish_connection(self) -> Tuple[socket.socket,Tuple[str,int]]:
        connection, addr = self.__ui_socket.accept()
        self.__logger.info("TCP connection is established with user {}:{}.".format(*addr))
        return connection, addr

    def __receive_request(self, connection:socket.socket, addr:Tuple[str,int]) -> str:
        user_request = connection.recv(BUFFER_SIZE).decode(ENCODING)
        self.__logger.info("Request  '{}' received from {}:{}.".format(user_request, *addr))
        return user_request

    def __handle_request(self, request:str) -> str:
        return "SUCCESS"

    def __send_ack(self, connection:socket.socket, addr:Tuple[str,int], ack:str) -> None:
        connection.send(ack.encode(ENCODING))
        self.__logger.info("User acknowledged '{}'".format(ack))
        connection.close()
        self.__logger.info("TCP connection is closed with user {}:{}.".format(*addr))


class CommunicationHandler:
    def __configure(self, config_path:str, client_id:str) -> None:
        topology_config:List[dict] = json.load(open(config_path, 'r'))
        for node_config in topology_config:
            if node_config.get("id") == client_id:
                self.__host, self.__port = node_config.get("host"), node_config.get("port")
                neighbour_nodes:List[dict] = node_config.get("connections")
                for neighbour in neighbour_nodes:
                    addr = (neighbour.get("host"), neighbour.get("port"))
                    self.__neighbours_addr.append(addr)

    def __init__(self, config_path:str, client_id:str) -> None:
        self.__config_path = config_path
        self.__client_id = client_id
        self.__host, self.__port = str(), int() 
        self.__neighbours_addr:List[Tuple[str,int]] = list()
        self.__configure(self.__config_path, self.__client_id)
        self.__cluster_handler = ClusterHandler(self.__host, self.__port, self.__neighbours_addr)
        self.__user_request_handler = UserRequestHandler(self.__host, self.__port+1)
        self.__logger = Logger("Communication-Handler")

    def get_address(self) -> Tuple[Tuple[str,int],Tuple[str,int]]:
        return (self.__host, self.__port), (self.__host, self.__port+5)

    def start(self) -> None:
        self.__logger.info("Started.")
        self.__cluster_handler.start()
        self.__user_request_handler.start()
        