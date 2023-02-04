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
            return response.success

        except Exception as e: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return False
        
    def __call_read_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, event:Events.READ, key:bytes) -> Tuple[bool, bytes]:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Read(pirestore_pb2.Request(
                id=request_id, command=event.value,
                key=key, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            return response.success, response.value

        except Exception as e: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return False, None

    def _create_main_protocol(self, request_id:int, replica_no:int, key:bytes, value:bytes) -> bool:
        # Send execute message to only one neighbour
        random.shuffle(self.__neighbours_addr)

        for dst_addr in self.__neighbours_addr:
            success = self.__call_create_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no,
                Events.CREATE, key, value)
            self.__logger.info("Create-Main-Protocol is on action, sending '{}' to {}:{}...".format(
                Events.CREATE.value, *dst_addr))
                
            if success: # Executed successfully, move the address to back
                replica_no += 1
                self.__neighbours_addr.remove(dst_addr)
                self.__neighbours_addr.append(dst_addr)
                self.__logger.info("Pair '{}:{}' is created on {}:{} ".format(
                    key.decode(ENCODING), value.decode(ENCODING), *dst_addr))
                break

        # Send redirect message to only one neighbour
        for dst_addr in self.__neighbours_addr:
            success = self.__call_create_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no,
                Events.CREATE_REDIR, key, value)
            self.__logger.info("Create-Main-Protocol is on action, sending '{}' to {}:{}...".format(
                Events.CREATE_REDIR.value, *dst_addr))

            if success: # Redirected successfully
                self.__logger.info("Pair '{}:{}' is redirected to {}:{} ".format(
                    key.decode(ENCODING), value.decode(ENCODING), *dst_addr))
                return True
        
        return False
    
    def _read_main_protocol(self, request_id:int, key:bytes) -> Tuple[bool, bytes]:
        random.shuffle(self.__neighbours_addr)
        for dst_addr in self.__neighbours_addr:
            success, read_value = self.__call_read_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, Events.READ, key)
            self.__logger.info("Read-Main-Protocol is on action, sending '{}' to {}:{}...".format(
                Events.READ.value, *dst_addr))
            if success: # Found
                self.__logger.info("Pair '{}:{}' is found in {}:{}".format(
                    key.decode(ENCODING), read_value.decode(ENCODING), self.__host, self.__grpc_port))
                return success, read_value
        return False, None

    def _create_replication_protocol(self, request:pirestore_pb2.Request) -> bool:
        # Send execute message to only one neighbour
        random.shuffle(self.__neighbours_addr)
        replica_no = request.replica_no

        create_only_success = False
        for dst_addr in self.__neighbours_addr:
            success = self.__call_create_service(
                (request.source.host, request.source.port), dst_addr,
                request.id, replica_no,
                Events.CREATE, request.key, request.value)
            self.__logger.info("Create-Redirect-Protocol is on action, sending '{}' to {}:{}...".format(
                Events.CREATE.value, *dst_addr))
                
            if success: # Executed successfully, move the address to back
                replica_no += 1
                self.__neighbours_addr.remove(dst_addr)
                self.__neighbours_addr.append(dst_addr)
                create_only_success = True
                self.__logger.info("Pair '{}:{}' is created on {}:{} ".format(
                    request.key.decode(request.encoding), request.value.decode(request.encoding), *dst_addr))
                break
        
        if replica_no < N_REPLICAS: # Redirect to one neighbour
            for dst_addr in self.__neighbours_addr:
                success = self.__call_create_service(
                    (self.__host, self.__grpc_port), dst_addr,
                    request.id, replica_no,
                    Events.CREATE_REDIR, request.key, request.value)
                self.__logger.info("Create-Redirect-Protocol is on action, sending '{}' to {}:{}...".format(
                    Events.CREATE_REDIR, *dst_addr))
                if success:
                    self.__logger.info("Pair '{}:{}' is redirected to {}:{} ".format(
                        request.key.decode(request.encoding), request.value.decode(request.encoding), *dst_addr))
                    return True # Both created and redirected
            return False # Could not redirected, though it is necessary

        return create_only_success

    def accept_greeting(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.insecure_channel(addr_as_str(*addr))
        self.__neighbours.update({addr:channel})
        self.__logger.info("Greeted with {}:{}.".format(*addr))

    def run_main_protocol(self, request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> Tuple[bool, bytes]:
        success = False
        read_value = None

        if event == Events.CREATE:
            success = self._create_main_protocol(request_id, replica_no, key, value)

        elif event == Events.READ:
            success, read_value = self._read_main_protocol(request_id, key)

        return success, read_value

    def run_replication_protocol(self, request:pirestore_pb2.Request) -> bool:
        success = False
        if request.command == Events.CREATE_REDIR.value:
            success = self._create_replication_protocol(request)
        return success

    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:channel})
        self.__send_greetings()
