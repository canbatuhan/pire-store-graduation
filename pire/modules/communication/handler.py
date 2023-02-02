import json
from typing import Tuple, List

from pire.modules.communication.cluster import ClusterHandler
from pire.modules.communication.user import UserHandler
from pire.util.logger import Logger


class CommunicationHandler:
    def __configure(self, client_id:str, config_path:str) -> None:
        topology_config:List[dict] = json.load(open(config_path, 'r'))
        for node_config in topology_config:
            if node_config.get("id") == client_id:
                self.__host, self.__port = node_config.get("host"), node_config.get("port")
                neighbour_nodes:List[dict] = node_config.get("connections")
                for neighbour in neighbour_nodes:
                    addr = (neighbour.get("host"), neighbour.get("port"))
                    self.__neighbours_addr.append(addr)

    def __init__(self, client_id:str, config_path:str) -> None:
        self.__client_id = client_id
        self.__config_path = config_path
        self.__host, self.__port = str(), int() 
        self.__neighbours_addr:List[Tuple[str,int]] = list()
        self.__configure(self.__client_id, self.__config_path)
        self.cluster_handler = ClusterHandler(self.__client_id, self.__host, self.__port, self.__neighbours_addr)
        self.user_request_handler = UserHandler(self.__client_id, self.__host, self.__port+5)
        self.__logger = Logger("Communication-Handler", self.__client_id)

    def get_address(self) -> Tuple[Tuple[str,int],Tuple[str,int]]:
        return (self.__host, self.__port), (self.__host, self.__port+5)

    def start(self) -> None:
        self.__logger.info("Started.")
        self.cluster_handler.start()
        self.user_request_handler.start()