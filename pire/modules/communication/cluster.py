import random
import grpc
from typing import Dict, Iterable, List, Tuple

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.util.constants import ENCODING, MAX_ID, N_REPLICAS
from pire.util.enums import Events
from pire.util.logger import Logger


class ClusterHandler:
    def __init__(self, client_id:str, host:str, grpc_port:int, neighbours_addr:List[Tuple[str,int]]) -> None:
        self.__host = host
        self.__grpc_port = grpc_port
        self.__neighbours_addr = neighbours_addr
        self.__neighbours:Dict[Tuple[str,int],grpc.Channel] = dict()
        self.__logger = Logger("Communication-Handler-Cluster-Handler", client_id)

    def __send_greetings(self) -> None:
        for addr, channel in self.__neighbours.items():
            try: # Try to send a message
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

    def __call_create_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> bool:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Create(pirestore_pb2.Request(
                id=request_id, replica_no=replica_no,
                command=event.value, key=key, value=value, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            
            if response.success and event == Events.CREATE:
                self.__logger.info("Pair '{}:{}' is created in {}:{} ".format(
                    key.decode(ENCODING), value.decode(ENCODING), *dst_addr))
                
            return response.success

        except: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return False
        
    def __call_read_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, event:Events, key:bytes) -> Tuple[bool, bytes]:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Read(pirestore_pb2.Request(
                id=request_id, command=event.value,
                key=key, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            
            if response.success:
                self.__logger.info("Pair '{}:{}' is read from {}:{} ".format(
                    key.decode(ENCODING), response.value.decode(ENCODING), *dst_addr))
                
            return response.success, response.value

        except: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return False, None

    def _create_protocol(self, request_id:int, replica_no:int, key:bytes, value:bytes) -> bool:
        # Send CREATE message to one of the neighbours
        random.shuffle(self.__neighbours_addr)
        success = False

        for dst_addr in self.__neighbours_addr:
            success = self.__call_create_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no,
                Events.CREATE, key, value)
                
            if success: # Executed successfully, move the address to back
                replica_no += 1 # Increment replica_no
                self.__neighbours_addr.remove(dst_addr)
                self.__neighbours_addr.append(dst_addr)
                break
        
        if replica_no < N_REPLICAS: # If there are replicas need to be created
            # Redirect CREATE message to one of the neighbours
            for dst_addr in self.__neighbours_addr:
                success = self.__call_create_service(
                    (self.__host, self.__grpc_port), dst_addr,
                    request_id, replica_no,
                    Events.CREATE_REDIR, key, value)

                if success: # All replicas are created 
                    break
        
        return success # All replicas are created
    
    def _read_protocol(self, request_id:int, key:bytes) -> Tuple[bool, bytes]:
        random.shuffle(self.__neighbours_addr)
        success = False
        read_value = None

        for dst_addr in self.__neighbours_addr:
            success, read_value = self.__call_read_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, Events.READ, key)
            if success: # Value found
                break
        return success, read_value
    
    def _update_protocol() -> None:
        pass

    def accept_greeting(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.insecure_channel(addr_as_str(*addr))
        self.__neighbours.update({addr:channel})
        self.__logger.info("Greeted with {}:{}.".format(*addr))

    def run_protocol(self, request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> Tuple[bool, bytes]:
        success = False
        read_value = None

        if event == Events.CREATE or event == Events.CREATE_REDIR:
            success = self._create_protocol(request_id, replica_no, key, value)

        elif event == Events.READ:
            success, read_value = self._read_protocol(request_id, key)

        elif event == Events.UPDATE:
            success = self._update_protocol(request_id, key)

        return success, read_value

    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:channel})
        self.__send_greetings()
