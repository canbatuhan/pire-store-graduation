import grpc
import json

from pire.modules.server import pirestore_pb2
from pire.modules.server import pirestore_pb2_grpc
from pire.util.logger import Logger

class CommunicationHandler:
    def __configure(self, config_path:str, client_id:str) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        topology_config = list[dict](json.load(open(config_path, 'r')))
        for node_config in topology_config:
            if node_config.get("id") == client_id:
                self.__addr = (node_config.get("host"), node_config.get("port"))
                neighbour_nodes = list[dict](node_config.get("connections"))
                for neighbour in neighbour_nodes:
                    try:
                        neighbour_addr = (neighbour.get("host"), neighbour.get("port"))
                        neighbour_channel = grpc.insecure_channel(addr_as_str(neighbour_addr))
                        self.__neighbours_addrs.append(neighbour_addr)
                        self.__neighbours_channels.append(neighbour_channel)
                        self.__logger.success("Connected to {}:{}.".format(*neighbour_addr))
                    except:
                        self.__logger.failure("Failed to connect to {}:{}. Node might be unawake.".format(*neighbour_addr))

    def __init__(self, config_path:str, client_id:str) -> None:
        self.__config_path = config_path
        self.__client_id = client_id
        self.__addr = tuple[str,int]()
        self.__neighbours_addrs = list[tuple[str,int]]()
        self.__neighbours_channels = list[grpc.Channel]()
        self.__logger = Logger("Communication-Handler")

    def get_address(self) -> tuple[str,int]:
        return self.__addr

    def start(self) -> None:
        self.__logger.info("Started.")
        self.__configure(self.__config_path, self.__client_id)