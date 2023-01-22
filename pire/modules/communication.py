import grpc

from pire.modules.server import pirestore_pb2
from pire.modules.server import pirestore_pb2_grpc
from pire.util.logger import Logger

class CommunicationHandler:
    def __configure(self) -> None:
        pass

    def __init__(self, config_path:str, client_id:str) -> None:
        self.__addr = tuple[str,int]()
        self.__neighbours_addr = list[tuple[str,int]]()
        self.__neighbours_channel = list[grpc.Channel]()
        self.__logger = Logger("Communication-Handler")

    def start(self) -> None:
        self.__logger.info("Started.")