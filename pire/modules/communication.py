import grpc
import socket
import json

from pire.modules.server import pirestore_pb2
from pire.modules.server import pirestore_pb2_grpc
from pire.util.logger import Logger
from pire.util.constants import BUFFER_SIZE, ENCODING

class CommunicationHandler:
    def __configure(self, config_path:str, client_id:str) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        topology_config = list[dict](json.load(open(config_path, 'r')))

        for node_config in topology_config:
            if node_config.get("id") == client_id:
                self.__host, self.__port = node_config.get("host"), node_config.get("port")
                neighbour_nodes = list[dict](node_config.get("connections"))
                for neighbour in neighbour_nodes:
                    try: # Try to connect
                        neighbour_addr = (neighbour.get("host"), neighbour.get("port"))
                        neighbour_channel = grpc.insecure_channel(addr_as_str(neighbour_addr))
                        self.__neighbours.update({neighbour_addr:neighbour_channel})
                        self.__logger.success("Connected to {}:{}.".format(*neighbour_addr))
                    except: # Node is not awake
                        self.__logger.failure("Failed to connect to {}:{}. Node might be unawake.".format(*neighbour_addr))

    def __init_socket(self) -> None:
        self.__ui_socket.bind((self.__host, self.__ui_port))
        self.__ui_socket.listen()
        self.__logger.info("UI socket is listening on {}:{}.".format(self.__host, self.__ui_port))

    def __send_greetings(self) -> None:
        for addr, channel in self.__neighbours.items():
            greeting_stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = greeting_stub.Greet(pirestore_pb2.Greeting(
                source=pirestore_pb2.Address(host=self.__host, port=self.__port),
                destination=pirestore_pb2.Address(host=addr[0], port=addr[1])
            ))

            if response.success:
                self.__logger.info("Greeted with {}:{}.".format(addr[0], addr[1]))
            
            else: # Unable to greet
                self.__logger.failure("Could not greeted with {}:{}.".format(addr[0], addr[1]))

    def __init__(self, config_path:str, client_id:str) -> None:
        self.__config_path = config_path
        self.__client_id = client_id
        self.__host = str()
        self.__grpc_port = int()
        self.__ui_port = 8080
        self.__ui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__neighbours = dict()
        self.__logger = Logger("Communication-Handler")

    def get_address(self) -> tuple[str,int]:
        return self.__host, self.__grpc_port

    def start(self) -> None:
        self.__logger.info("Started.")
        self.__configure(self.__config_path, self.__client_id)
        self.__init_socket()
        self.__send_greetings()

    def establish_connection(self) -> tuple[socket.socket,tuple[str,int]]:
        connection, addr = self.__ui_socket.accept()
        self.__logger.info("TCP connection is established with user {}:{}.".format(addr[0], addr[1]))
        return connection, addr

    def receive_request(self, connection:socket.socket) -> str:
        user_request = connection.recv(BUFFER_SIZE).decode(ENCODING)
        self.__logger.info("Request received from {}:{} '{}'".format(user_request))
        return user_request

    def handle_request(self, request:str) -> str:
        return "SUCCESS"

    def send_ack(self, connection:socket.socket, addr:tuple[str,int], ack:str) -> None:
        connection.send(ack.encode(ENCODING))
        self.__logger.info("User acknowledged '{}'".format(ack))
        connection.close()
        self.__logger.info("TCP connection is closed with user {}:{}.".format(addr[0], addr[1]))
        