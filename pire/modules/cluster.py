import random
import grpc
from typing import Dict, List, Tuple

from pire.modules.discovery import DestinationInfo, DiscoveryItem
from pire.modules.service   import pirestore_pb2
from pire.modules.service   import pirestore_pb2_grpc

class ClusterHandler:
    MAX_REPLICAS = int()
    MIN_REPLICAS = int()
    
    def __init__(self, host:str, port:int, neighbours:List[Tuple[str,int]], max_replicas:int, min_replicas:int) -> None:
        self.__host       = host
        self.__port       = port
        self.__neighbours = neighbours
        self.__stub_map:Dict[Tuple[str,int], pirestore_pb2_grpc.PireStoreStub] = dict()
        self.__discovery_map:Dict[bytes, DiscoveryItem]                        = dict()
        self.__created:List[pirestore_pb2.PairInfo] = list()
        self.__deleted:List[pirestore_pb2.PairInfo] = list()
        self.MAX_REPLICAS = max_replicas
        self.MIN_REPLICAS = min_replicas

    """ GREET Protocol Implementation Starts """

    def greet_protocol_sender(self) -> None:
        for _, stub in self.__stub_map.items():
            try: # Try to send a message
                _ = stub.Greet(pirestore_pb2.Greeting())
            except Exception as exception:
                print(exception.with_traceback())
                pass # Channel is broken or error in code
    
    def greet_protocol_receiver(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.insecure_channel(addr_as_str(*addr))
        self.__neighbours[addr] = pirestore_pb2_grpc.PireStoreStub(channel)

    """ GREET Protocol Implementation Ends """

    
    """ DISCOVER Protocol Implementation Starts """

    def discover_protocol_sender(self) -> None:
        for _, stub in self.__stub_map.items():
            try: # Try to send a message
                addr_grpc = pirestore_pb2.Address(
                    host=self.host, port=self.port)
                
                # Call gRPC Service "Discovery"
                grpc_discovery = pirestore_pb2.Discovery(address=addr_grpc)
                grpc_discovery.created.extend(self.__created)
                grpc_discovery.deleted.extend(self.__deleted)
                _ = stub.Greet(grpc_discovery)

                # Clear waiting discovery messages
                self.__created.clear()
                self.__deleted.clear()
                
            except Exception as exception:
                print(exception.with_traceback())
                pass # Channel is broken or error in code

    def discover_protocol_receiver(self, next_addr:Tuple[str,int], created:List[pirestore_pb2.PairInfo], deleted:List[pirestore_pb2.PairInfo]) -> None:
        # Process created pairs
        for pair_info in created:
            owner_addr     = (pair_info.owner.host, pair_info.owner.port)
            key            = pair_info.key
            hops           = pair_info.hops
            discovery_item = self.__discovery_map.get(key)

            # First hearing of the key
            if discovery_item == None:
                self.__discovery_map[key] = DiscoveryItem()

            # Try to set
            is_set = self.__discovery_map[key].set_destination(owner_addr, next_addr, hops+1)
            if is_set: # Destinations are updated
                owner_addr_grpc = pirestore_pb2.Address(
                    host=pair_info.owner.host, port=pair_info.owner.port)
                self.__created.append(pirestore_pb2.PairInfo(
                    key=key, owner=owner_addr_grpc, hops=hops+1))

        # Process deleted pairs
        for pair_info in deleted:
            owner_addr     = (pair_info.owner.host, pair_info.owner.port)
            key            = pair_info.key
            hops           = pair_info.hops
            discovery_item = self.__discovery_map.get(key)

            # Pair exists
            if discovery_item != None:
                is_deleted, remaining = self.__discovery_map[key].delete_destination(owner_addr)
                if is_deleted: # Destinations are updated
                    owner_addr_grpc = pirestore_pb2.Address(
                        host=pair_info.owner.host, port=pair_info.owner.port)
                    self.__deleted.append(pirestore_pb2.PairInfo(
                        key=key, owner=owner_addr_grpc, hops=hops+1))
                    
                # All possible destinations are deleted
                if remaining == 0:
                    self.__discovery_map.pop(key)


    """ DISCOVER Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    def __call_Create(self) -> int:
        pass

    def create_protocol(self) -> Tuple[bool, int]:
        pass

    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """

    def __call_Read(self) -> int:
        pass

    def read_protocol(self) -> Tuple[bool, int]:
        pass

    """ READ Protocol Implementation Ends """


    """ UPDATE Protocol Implementation Starts """

    def __call_Update(self) -> int:
        pass 

    def update_protocol() -> Tuple[bool, int]:
        pass

    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """

    def __call_Delete(self) -> int:
        pass 

    def delete_protocol() -> Tuple[bool, int]:
        pass

    """ DELETE Protocol Implementation Ends """