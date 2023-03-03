import json
from typing import Tuple, List

from pire.modules.communication.cluster import ClusterHandler
from pire.modules.communication.user import UserHandler
from pire.util.logger import Logger


class CommunicationHandler:
    def __get_neighbour_addrs(self) -> List[Tuple[str,int]]:
        neighbours_addr:List[Tuple[str,int]] = list()
        topology_config:List[dict] = json.load(open(self.__config_path, 'r'))
        for node_config in topology_config:
            if node_config.get("id") == self.__client_id:
                self.__host, self.__port = node_config.get("host"), node_config.get("port")
                neighbour_nodes:List[dict] = node_config.get("connections")
                for neighbour in neighbour_nodes:
                    addr = (neighbour.get("host"), neighbour.get("port"))
                    neighbours_addr.append(addr)
                break
        return neighbours_addr

    def __init__(self, client_id:str, config_path:str) -> None:
        self.__client_id = client_id
        self.__config_path = config_path
        self.__host, self.__port = str(), int() 
        self.cluster_handler = ClusterHandler(self.__host, self.__port, self.__get_neighbour_addrs())
        self.user_request_handler = UserHandler(self.__host, self.__port+1000)
        self.__logger = Logger("Communication-Handler")

    def get_address(self) -> Tuple[Tuple[str,int],Tuple[str,int]]:
        return (self.__host, self.__port), (self.__host, self.__port+5)

    def start(self) -> None:
        self.__logger.info("Started.")
        self.cluster_handler.start()
        self.user_request_handler.start()