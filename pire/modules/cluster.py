import random
import grpc
from typing import Dict, List, Tuple

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc

class ClusterHandler:
    MAX_REPLICAS = int()
    MIN_REPLICAS = int()
    
    def __init__(self, neighbours:List[Tuple[str,int]], max_replicas:int, min_replicas:int) -> None:
        self.__neighbours = neighbours
        self.owner_map:Dict[str, List[Tuple[str,int]]] = dict()
        self.__stub_map:Dict[Tuple[str,int], pirestore_pb2_grpc.PireStoreStub] = dict()
        self.MAX_REPLICAS = max_replicas
        self.MIN_REPLICAS = min_replicas

    async def start(self, host, port) -> None:
        await self.greet_protocol_sender(host, port)

    """ GREET Protocol Implementation Starts """

    async def greet_protocol_sender(self, host:str, port:int) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        for addr in self.__neighbours:
            try: # Try to send a message
                stub = pirestore_pb2_grpc.PireStoreStub(
                    grpc.aio.insecure_channel(addr_as_str(*addr)))
                
                grpc_greet = pirestore_pb2.GreetProtocolMessage(
                    sender = pirestore_pb2.Address(
                        host = host,
                        port = port
                    )
                )
                
                _ = await stub.Greet(grpc_greet)
                self.__stub_map.update({addr:stub})

            except: # Channel is broken or error in code
                pass
    
    async def greet_protocol_receiver(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        stub = pirestore_pb2_grpc.PireStoreStub(
            grpc.aio.insecure_channel(addr_as_str(*addr)))
        self.__stub_map.update({addr:stub})

    """ GREET Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    async def __call_Create(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.WriteProtocolMessage) -> Tuple[int,List[pirestore_pb2.Address]]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = await stub.Create(request)
            return response.ack, response.visited
        
        except: # Channel is broken or error in the code
            return request.metadata.replica, request.metadata.visited

    async def create_protocol(self, request:pirestore_pb2.WriteProtocolMessage) -> Tuple[bool,int]:
        visited_addrs:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                              for each in request.metadata.visited]
        
        random.shuffle(self.__neighbours)
        for addr in self.__neighbours:
            if addr not in visited_addrs: # Unvisited neighbour
                ack, visited = await self.__call_Create(addr, request)
                if ack > request.metadata.replica:
                    del request.metadata.visited[:]
                    request.metadata.visited.extend(visited)
                    request.metadata.replica = ack

                    # Remember the pair's location
                    pair = self.owner_map.get(request.payload.key)
                    if pair == None: # First record of the entry
                        self.owner_map.update({request.payload.key: [addr]})

                    else: # Entry already exists
                        pair.append(addr)
            
            if request.metadata.replica == self.MAX_REPLICAS:
                break # Halt
        
        # Satisfaction of the criteria and the most recent replica number
        return request.metadata.replica >= self.MIN_REPLICAS, request.metadata.replica

    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """

    async def __call_Read(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bool,str,List[pirestore_pb2.Address]]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = await stub.Read(request)
            return response.success, response.value, response.visited
        
        except Exception as e: # Channel is broken or error in the code
            return False, None, request.metadata.visited

    async def read_protocol(self, request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bool,str,List[pirestore_pb2.Address]]:
        visited_addrs:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                              for each in request.metadata.visited]
        success, value = False, None
        random.shuffle(self.__neighbours)
        for addr in self.__neighbours:
            if addr not in visited_addrs: # Unvisited neighbour
                success, value, visited = await self.__call_Read(addr, request)
                if not success: # Pair can not found
                    del request.metadata.visited[:]
                    request.metadata.visited.extend(visited)
                else: # Pair found
                    break

        return success, value, request.metadata.visited

    """ READ Protocol Implementation Ends """

    
    """ VALIDATE Protocol Implementation Starts """

    async def __call_Validate(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bytes,int]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = await stub.Validate(request)
            return response.value, response.version
        
        except Exception as e: # Channel is broken or error in the code
            print("__call_Validate:", e.with_traceback(None))
            return request.payload.value, response.payload.version

    async def validate_protocol(self, request) -> Tuple[str,int]:
        owner_neighbours = self.owner_map.get(request.payload.key)
        if owner_neighbours != None: # The pair is stored in a neighbour
            addr = random.choice(owner_neighbours)
            val_value, val_version = await self.__call_Validate(addr, request)
            return val_value, val_version
        else: # No neighbours storing the pair (unusual)
            return request.payload.value, request.payload.version

    """ VALIDATE Protocol Implementation Starts """


    """ UPDATE Protocol Implementation Starts """

    async def __call_Update(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.WriteProtocolMessage) -> Tuple[int,List[pirestore_pb2.Address]]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Update(request)
            return response.replica, response.visited
        
        except: # Channel is broken or error in the code
            return request.metadata.replica, request.metadata.visited

    async def update_protocol(self, request) -> Tuple[int,List[pirestore_pb2.Address]]:
        visited_addrs:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                              for each in request.metadata.visited]
        """
        neighbours_traverse = self.owner_map.get(request.payload.key)
        if neighbours_traverse == None: # The node is not an owner
            random.shuffle(self.__neighbours)
            neighbours_traverse = self.__neighbours

        for addr in neighbours_traverse:
            if addr not in visited_addrs:
                ack, visited = await self.__call_Update(addr, request)
                
                if ack > request.metadata.replica:
                    request.metadata.replica = ack
                    request.metadata.visited = visited

            if request.metadata.replica == self.MAX_REPLICAS:
                break # Halt
        """
        random.shuffle(self.__neighbours)
        for addr in self.__neighbours:

            if addr not in visited_addrs:
                ack, visited = await self.__call_Update(addr, request)
                del request.metadata.visited[:]
                request.metadata.visited.extend(visited)

                if ack: # Updated in the neighbour
                    request.metadata.replica = ack
                    break # Halt

        return request.metadata.replica, request.metadata.visited
   
    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """

    async def __call_Delete(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.WriteProtocolMessage) -> int:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Delete(request)
            return response.replica, response.visited
        
        except: # Channel is broken or error in the code
            return request.metadata.replica, request.metadata.visited   

    async def delete_protocol(self, request) -> Tuple[bool,int,List[pirestore_pb2.Address]]:
        visited_addrs:List[Tuple[str,int]] = [(each.host, each.port) # Format conversion
                                              for each in request.metadata.visited]
        
        neighbours_traverse = self.owner_map.get(request.payload.key)
        was_owner = True
        if neighbours_traverse == None: # The node is not an owner
            random.shuffle(self.__neighbours)
            neighbours_traverse = self.__neighbours
            was_owner = False

        for addr in neighbours_traverse:
            if addr not in visited_addrs:
                ack, visited = await self.__call_Delete(addr, request)
                
                if ack > request.metadata.replica:
                    request.metadata.replica = ack
                    del request.metadata.visited[:]
                    request.metadata.visited.extend(visited)

            if request.metadata.replica == self.MAX_REPLICAS:
                break # Halt
        
        if was_owner: # Remove entry if one of the owners
            self.owner_map.pop(request.payload.key)

        return request.metadata.replica >= self.MIN_REPLICAS, request.metadata.replica, request.metadata.visited

    """ DELETE Protocol Implementation Ends """