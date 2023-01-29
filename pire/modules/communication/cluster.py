import grpc
from typing import Dict, Iterable, Tuple

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.util.enums import Events
from pire.util.exceptions import ServiceUndefinedException
from pire.util.logger import Logger


class ClusterHandler:
    def __init__(self, client_id:str, host:str, grpc_port:int, neighbours_addr:Iterable[Tuple[str,int]]) -> None:
        self.__host = host
        self.__grpc_port = grpc_port
        self.__neighbours_addr = neighbours_addr
        self.__neighbours:Dict[Tuple[str,int],grpc.Channel] = dict()
        self.__logger = Logger("Communication-Handler-Cluster-Handler", client_id)

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

    def accept_greeting_callback(self, addr:Tuple[str,int]) -> None:
        self.__logger.info("Greeted with {}:{}.".format(*addr))

    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:channel})
        self.__send_greetings()

    def execute(self, event:Events, key:str, value:object) -> str:
        pass