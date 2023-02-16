import random
import grpc
from typing import Dict, List, Tuple

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.util.constants import ENCODING, N_REPLICAS
from pire.util.enums import Events
from pire.util.logger import Logger


class ClusterHandler:
    def __init__(self, client_id:str, host:str, grpc_port:int, neighbours_addr:List[Tuple[str,int]]) -> None:
        self.__host = host
        self.__grpc_port = grpc_port
        self.__neighbours_addr = neighbours_addr
        self.__neighbours:Dict[Tuple[str,int],grpc.Channel] = dict()
        self.__logger = Logger("Communication-Handler-Cluster-Handler", client_id)


    """ GREET Protocol Implementation Starts """

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
        
    def accept_greeting(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.insecure_channel(addr_as_str(*addr))
        self.__neighbours.update({addr:channel})
        self.__logger.info("Greeted with {}:{}.".format(*addr))

    """ GREET Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    def __call_create_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> bool:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Create(pirestore_pb2.WriteRequest(
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

    def create_protocol(self, request_id:int, replica_no:int, key:bytes, value:bytes) -> bool:
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
    
    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """
    
    def __call_read_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, key:bytes) -> Tuple[bool, bytes]:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Read(pirestore_pb2.ReadRequest(
                id=request_id, command=Events.READ.value,
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

    def read_protocol(self, request_id:int, key:bytes) -> Tuple[bool, bytes]:
        random.shuffle(self.__neighbours_addr)
        success = False
        read_value = None

        for dst_addr in self.__neighbours_addr:
            success, read_value = self.__call_read_service(
                (self.__host, self.__grpc_port), dst_addr, request_id, key)

            if success: # Value found
                break

        return success, read_value
    
    """ READ Protocol Implementation Ends """


    """ UPDATE Protocol Implementation Starts """
    
    def __call_update_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, key:bytes, value:bytes) -> int:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Update(pirestore_pb2.WriteRequest(
                id=request_id, replica_no=replica_no,
                command=Events.UPDATE.value,
                key=key, value=value, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            
            if response.ack_no > replica_no:
                self.__logger.info("Pair '{}:{}' is updated in {}:{} ".format(
                    key.decode(ENCODING), value.decode(ENCODING), *dst_addr))
                
            return response.ack_no

        except Exception as e: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return replica_no

    def update_protocol(self, request_id:int, replica_no:int, key:bytes, value:bytes) -> Tuple[bool, int]:
        # Send CREATE message to one of the neighbours
        random.shuffle(self.__neighbours_addr)

        for dst_addr in self.__neighbours_addr:
            ack_no = self.__call_update_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no, key, value)

            if ack_no > replica_no:
                replica_no = ack_no # Next replica id to update

            if ack_no == N_REPLICAS:
                break # All replicas are updated

        return replica_no == N_REPLICAS, ack_no 
    
    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """
    
    def __call_delete_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, key:bytes) -> int:
        try: # Try to send a message
            channel = self.__neighbours.get(dst_addr)
            stub = pirestore_pb2_grpc.PireKeyValueStoreStub(channel)
            response = stub.Delete(pirestore_pb2.WriteRequest(
                id=request_id, replica_no=replica_no,
                command=Events.DELETE.value,
                key=key, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            
            if response.ack_no > replica_no:
                self.__logger.info("Key '{}' is deleted in {}:{} ".format(
                    key.decode(ENCODING), *dst_addr))
                
            return response.ack_no

        except Exception as e: # Channel is broken
            self.__logger.failure("gRPC channel with {}:{} is not available. Node might be unawake".format(*dst_addr))
            return replica_no

    def delete_protocol(self, request_id:int, replica_no:int, key:bytes) -> Tuple[bool, int]:
        # Send CREATE message to one of the neighbours
        random.shuffle(self.__neighbours_addr)
        
        for dst_addr in self.__neighbours_addr:
            ack_no = self.__call_delete_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no, key)

            if ack_no > replica_no:
                replica_no = ack_no # Next replica id to update

            if ack_no == N_REPLICAS:
                break # All replicas are updated

        return replica_no == N_REPLICAS, ack_no 
    
    """ DELETE Protocol Implementation Ends """


    def run_protocol(self, request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> Tuple[bool, object]:
        if event == Events.CREATE or event == Events.CREATE_REDIR:
            success = self.create_protocol(
                request_id, replica_no, key, value)
            return success, None

        elif event == Events.READ:
            success, read_value = self.read_protocol(
                request_id, key)
            return success, read_value

        elif event == Events.UPDATE:
            success, ack_no = self.update_protocol(
                request_id, replica_no, key, value)
            return success, ack_no
        
        elif event == Events.DELETE:
            success, ack_no = self.delete_protocol(
                request_id, replica_no, key)
            return success, ack_no


    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:channel})
        self.__send_greetings()
