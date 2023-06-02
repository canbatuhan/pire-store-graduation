from enum import Enum
import random
import grpc
from typing import Dict, List, Tuple

from pire.modules.discovery import DiscoveryItem
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
        self.__newly_created:List[pirestore_pb2.PairInfo] = list()
        self.__newly_deleted:List[pirestore_pb2.PairInfo] = list()
        self.MAX_REPLICAS = max_replicas
        self.MIN_REPLICAS = min_replicas

    def get_address(self) -> Tuple[str,int]:
        return (self.__host, self.__port)
    
    def add_to_created(self, key:bytes, owner:Tuple[str,int]=None, next:Tuple[str,int]=None, hops:int=0) -> None:
        discovery_item = self.__discovery_map.get(key)

        # First hearing of the key
        if discovery_item == None:
            self.__discovery_map[key] = DiscoveryItem()

        # Owner is 'self'
        if owner == None:
            owner = (self.__host, self.__port)

        is_set = self.__discovery_map[key].set_destination(owner, next, hops)
        
        if is_set: # Set in the discovery map
            owner_addr_grpc = pirestore_pb2.Address(
                    host=owner[0], port=owner[1])
            self.__newly_created.append(pirestore_pb2.PairInfo(
                    key=key, owner=owner_addr_grpc, hops=hops))

    def add_to_deleted(self, key:bytes, owner:Tuple[str,int]=None, next:Tuple[str,int]=None, hops:int=0) -> None:
        discovery_item = self.__discovery_map.get(key)

        # First hearing of the key
        if discovery_item != None:

            # Owner is 'self'
            if owner == None:
                owner = (self.__host, self.__port)

            is_deleted, remaining = self.__discovery_map[key].delete_destination(owner)
            
            if is_deleted: # Deleted from the discovery map
                owner_addr_grpc = pirestore_pb2.Address(
                        host=owner[0], host=owner[1])
                self.__newly_deleted.append(pirestore_pb2.PairInfo(
                        key=key, owner=owner_addr_grpc, hops=hops))
                
            if remaining == 0:
                self.__discovery_map.pop(key)

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
                grpc_discovery.created.extend(self.__newly_created)
                grpc_discovery.deleted.extend(self.__newly_deleted)
                _ = stub.Greet(grpc_discovery)

                # Clear waiting discovery messages
                self.__newly_created.clear()
                self.__newly_deleted.clear()
                
            except Exception as exception:
                print(exception.with_traceback())
                pass # Channel is broken or error in code

    def discover_protocol_receiver(self, next_addr:Tuple[str,int], created:List[pirestore_pb2.PairInfo], deleted:List[pirestore_pb2.PairInfo]) -> None:
        # Process created pairs
        for pair_info in created:
            key        = pair_info.key
            owner_addr = (pair_info.owner.host, pair_info.owner.port)
            hops       = pair_info.hops
            self.add_to_created(key, owner_addr, next_addr, hops+1)

        # Process deleted pairs
        for pair_info in deleted:
            owner_addr = (pair_info.owner.host, pair_info.owner.port)
            key        = pair_info.key
            hops       = pair_info.hops
            self.add_to_deleted(key, owner_addr, next_addr, hops+1)


    """ DISCOVER Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    def __call_Create(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.CreateProtocolMessage) -> int:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Create(request)
            return response.replica
        
        # Channel is broken or error in code
        except Exception as exception:
            print(exception.with_traceback())
            return request.payload.replica 

    def create_protocol(self, request:pirestore_pb2.CreateProtocolMessage) -> Tuple[bool,int]:
        visited = [(each.host, each.port) for each in request.visited.vals]
        
        # Send a 'CREATE' request
        random.shuffle(self.__neighbours)
        request.direct = True # It is a direct request

        for neigh_addr in self.__neighbours:
            if neigh_addr not in visited:
                ack = self.__call_Create(neigh_addr, request)

                # Some replicas are created
                if ack > request.payload.replica:
                    request.payload.replica = ack # inplace!
                    request.visited.extend([pirestore_pb2.Address(
                        host=neigh_addr[0], port=neigh_addr[1])]) # inplace!
                    visited.append(neigh_addr)

                    # Move the neighbour the back of the list
                    self.__neighbours.remove(neigh_addr)
                    self.__neighbours.append(neigh_addr)
                    break

        # Send 'CREATE_REDIRECT' requests
        request.direct = False # It is a redirect request
        if request.payload.replica < self.MAX_REPLICAS:
            for neigh_addr in self.__neighbours:
                ack = self.__call_Create(neigh_addr, request)

                # Some replicas are created
                if ack > request.payload.replica: 
                    # Next replica to create
                    request.payload.replica = ack # inplace!

                # All replicas are created
                if request.payload.replica == self.MAX_REPLICAS:
                    break

        # Success criteria and the number of created replicas
        return request.payload.replica <= self.MIN_REPLICAS, request.payload.replica

    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """

    def __call_Read(self, neigh_addr:Tuple[str,int], request:pirestore_pb2.ReadProtocolMessage) -> Tuple[bool,int]:
        try: # Try to send a message
            stub = self.__stub_map.get(neigh_addr)
            response = stub.Read(request)
            return True, response.val
        
        # Channel is broken or error in code
        except Exception as exception:
            print(exception.with_traceback())
            return False, None

    def read_protocol(self, request) -> Tuple[bool,int]:
        success = False
        value   = None

        discovery_item = self.__discovery_map.get(request.key)

        # Key's location is not known
        if discovery_item == None:
            request.payload.blind = True
            for neigh_addr in self.__neighbours:
                success, value = self.__call_Read(neigh_addr, request)
                if success:
                    break # Value is found


        else: # Key's location is known
            # TODO : do not randomly choice from the best destination
            destination_info = discovery_item.get_best_destination()
            owner_addr = destination_info.owner
            next_addr = random.choice(destination_info.nexts)

            request.payload.blind = False
            request.dest = pirestore_pb2.Address(host=owner_addr[0], port=owner_addr[1])
            success, value = self.__call_Read(next_addr, request)

        return success, value

    """ READ Protocol Implementation Ends """


    """ UPDATE Protocol Implementation Starts """

    def __call_Update(self) -> int:
        pass 

    def update_protocol(self, request) -> Tuple[bool, int]:
        pass

    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """

    def __call_Delete(self) -> int:
        pass 

    def delete_protocol() -> Tuple[bool, int]:
        pass

    """ DELETE Protocol Implementation Ends """