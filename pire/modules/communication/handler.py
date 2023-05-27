import json
from typing import Tuple, List

from pire.modules.communication.cluster import ClusterHandler
from pire.modules.communication.user import UserHandler


class CommunicationHandler:
    def __get_neighbour_addrs(self) -> List[Tuple[str,int]]:
        neighbour_addrs:List[Tuple[str,int]] = list()
        topology_config:List[dict] = json.load(open(self.__config_path, 'r'))
        for node_config in topology_config:
            if node_config.get("id") == self.__client_id:
                self.__host, self.__port = node_config.get("host"), node_config.get("port")
                neighbour_nodes:List[dict] = node_config.get("connections")
                for neighbour in neighbour_nodes:
                    addr = (neighbour.get("host"), neighbour.get("port"))
                    neighbour_addrs.append(addr)
                break
        return neighbour_addrs

    def __init__(self, client_id:str, config_path:str) -> None:
        self.__client_id = client_id
        self.__config_path = config_path
        self.__host, self.__port = str(), int()
        neighbour_addrs = self.__get_neighbour_addrs()
        self.cluster_handler = ClusterHandler(self.__host, self.__port, neighbour_addrs)
        self.user_request_handler = UserHandler(self.__host, self.__port+1000)

    def get_address(self) -> Tuple[Tuple[str,int],Tuple[str,int]]:
        return (self.__host, self.__port), (self.__host, self.__port+5)

    def start(self) -> None:
        self.cluster_handler.start()
        self.user_request_handler.start()