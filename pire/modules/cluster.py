from enum import Enum
import random
import asyncio
import grpc
from typing import Dict, List, Tuple

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
        self.MAX_REPLICAS = max_replicas
        self.MIN_REPLICAS = min_replicas

    def get_address(self) -> Tuple[str,int]:
        return (self.__host, self.__port)

    """ GREET Protocol Implementation Starts """

    async def greet_protocol_sender(self) -> None:
        for _, stub in self.__stub_map.items():
            try: # Try to send a message
                grpcGreet = pirestore_pb2.GreetProtocolMessage()
                grpcGreet.sender = pirestore_pb2.Address(host=self.__host, port=self.__port)
                _ = await stub.Greet(grpcGreet)
            except Exception as exception:
                print(exception.with_traceback())
                pass # Channel is broken or error in code
    
    async def greet_protocol_receiver(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.aio.insecure_channel(addr_as_str(*addr))
        self.__neighbours[addr] = pirestore_pb2_grpc.PireStoreStub(channel)

    """ GREET Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    async def __call_Create(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.WriteProtocolMessage) -> Tuple[int,List:pirestore_pb2.Address]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = await stub.Create(request)
            return response.ack, response.visited
        
        # Channel is broken or error in the code
        except Exception as exception:
            print(exception.with_traceback())
            return request.metadata.replica, request.metadata.visited

    async def create_protocol(self, request:pirestore_pb2.WriteProtocolMessage) -> Tuple[bool,int]:
        visited:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                        for each in request.metadata.visited.vals]
        
        for addr in self.__neighbours:
            if addr not in visited:
                ack, visited = await self.__call_Create(request)
                if ack > request.metadata.replica:
                    request.metadata.visited = visited
                    request.metadata.replica = ack
            
            # Halt condition
            if request.metadata.replica == self.MAX_REPLICAS:
                break
        
        # Satisfaction of the criteria and the most recent replica number
        return request.metadata.replica >= self.MIN_REPLICAS, request.metadata.replica

    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """

    async def __call_Read(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bool,bytes,List:pirestore_pb2.Address]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = await stub.Read(request)
            return response.success, response.val, response.visited
        
        # Channel is broken or error in the code
        except Exception as exception:
            print(exception.with_traceback())
            return False, None, request.visited

    async def read_protocol(self, request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bool,bytes]:
        visited:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                        for each in request.metadata.visited.vals]
        
        for addr in self.__neighbours:
            if addr not in visited:
                success, val, visited = self.__call_Read(addr, request)
                if not success:
                    request.metadata.visited = visited
                else: # Pair found
                    break

        return success, val

    """ READ Protocol Implementation Ends """

    
    """ VALIDATE Protocol Implementation Starts """

    async def __call_Validate():
        pass

    async def validate_protocol(self, request) -> Tuple[bool,int]:
        pass

    """ VALIDATE Protocol Implementation Starts """


    """ UPDATE Protocol Implementation Starts """

    async def __call_Update(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.UpdateProtocolMessage) -> int:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Update(request)
            return response.replica
        
        # Channel is broken or error in code
        except Exception as exception:
            print(exception.with_traceback())
            return request.payload.replica  

    async def update_protocol(self, request) -> Tuple[bool, int]:
        pass

    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """

    async def __call_Delete(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.DeleteProtocolMessage) -> int:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Delete(request)
            return response.replica
        
        # Channel is broken or error in code
        except Exception as exception:
            print(exception.with_traceback())
            return request.payload.replica   

    async def delete_protocol(self, request) -> Tuple[bool, int]:
        pass

    """ DELETE Protocol Implementation Ends """