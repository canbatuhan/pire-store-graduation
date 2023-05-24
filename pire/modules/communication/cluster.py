import random
import grpc
from typing import Dict, List, Tuple

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.util.constants import ENCODING, MIN_REPLICAS, N_REPLICAS
from pire.util.enums import Events


class ClusterHandler:
    def __init__(self, host:str, grpc_port:int, neighbours_addr:List[Tuple[str,int]]) -> None:
        self.__host = host
        self.__grpc_port = grpc_port
        self.__neighbours_addr = neighbours_addr
        self.__neighbours:Dict[Tuple[str,int],pirestore_pb2_grpc.PireKeyValueStoreStub] = dict()


    """ GREET Protocol Implementation Starts """

    def __send_greetings(self) -> None:
        for addr, stub in self.__neighbours.items():
            try: # Try to send a message
                response = stub.Greet(pirestore_pb2.Greeting(
                    source=pirestore_pb2.Address(host=self.__host, port=self.__grpc_port),
                    destination=pirestore_pb2.Address(host=addr[0], port=addr[1])))
            
            except Exception: # Channel is broken or ERROR IN CODE!
                pass
        
    def accept_greeting(self, addr:Tuple[str,int]) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p)
        channel = grpc.insecure_channel(addr_as_str(*addr))
        self.__neighbours.update({addr:pirestore_pb2_grpc.PireKeyValueStoreStub(channel)})

    """ GREET Protocol Implementation Ends """


    """ CREATE Protocol Implementation Starts """

    def __call_create_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> bool:
        try: # Try to send a message
            stub = self.__neighbours.get(dst_addr)
            response = stub.Create(pirestore_pb2.WriteRequest(
                id=request_id, replica_no=replica_no,
                command=event.value, key=key, value=value, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            return response.ack_no

        except Exception: # Channel is broken or ERROR IN CODE!
            pass

    def create_protocol(self, request_id:int, replica_no:int, key:bytes, value:bytes) -> Tuple[bool, int]:
        # Send CREATE message to one of the neighbours
        random.shuffle(self.__neighbours_addr)

        for dst_addr in self.__neighbours_addr:
            ack_no = self.__call_create_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no,
                Events.CREATE, key, value)
                
            if ack_no == replica_no + 1: # A pair is created directly
                replica_no = ack_no # Next replica id to update
                self.__neighbours_addr.remove(dst_addr)
                self.__neighbours_addr.append(dst_addr)
                break
        
        # Send CREATE_REDIR message to neighbours
        if replica_no < N_REPLICAS: # If there are replicas need to be created
            for dst_addr in self.__neighbours_addr:
                ack_no = self.__call_create_service(
                    (self.__host, self.__grpc_port), dst_addr,
                    request_id, replica_no,
                    Events.CREATE_REDIR, key, value)

                if ack_no > replica_no:
                    replica_no = ack_no # Next replica id to create

                if ack_no == N_REPLICAS: 
                    break # All replicas are created 
        
        return replica_no >= MIN_REPLICAS, ack_no 
    
    """ CREATE Protocol Implementation Ends """


    """ READ Protocol Implementation Starts """
    
    def __call_read_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, key:bytes) -> Tuple[bool, bytes]:
        try: # Try to send a message
            stub = self.__neighbours.get(dst_addr)
            response = stub.Read(pirestore_pb2.ReadRequest(
                id=request_id, command=Events.READ.value,
                key=key, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            return response.success, response.value

        except Exception: # Channel is broken or ERROR IN CODE!
            pass

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
            stub = self.__neighbours.get(dst_addr)
            response = stub.Update(pirestore_pb2.WriteRequest(
                id=request_id, replica_no=replica_no,
                command=Events.UPDATE.value,
                key=key, value=value, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))     
            return response.ack_no

        except Exception: # Channel is broken or ERROR IN CODE!
            pass

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

        return replica_no >= MIN_REPLICAS, ack_no 
    
    """ UPDATE Protocol Implementation Ends """


    """ DELETE Protocol Implementation Starts """
    
    def __call_delete_service(self, src_addr:Tuple[str,int], dst_addr:Tuple[str,int], request_id:int, replica_no:int, key:bytes) -> int:
        try: # Try to send a message
            stub = self.__neighbours.get(dst_addr)
            response = stub.Delete(pirestore_pb2.WriteRequest(
                id=request_id, replica_no=replica_no,
                command=Events.DELETE.value,
                key=key, encoding=ENCODING,
                source=pirestore_pb2.Address(host=src_addr[0], port=src_addr[1]),
                destination=pirestore_pb2.Address(host=dst_addr[0], port=dst_addr[1])))
            return response.ack_no

        except Exception: # Channel is broken or ERROR IN CODE!
            pass

    def delete_protocol(self, request_id:int, replica_no:int, key:bytes) -> Tuple[bool, int]:
        # Send CREATE message to one of the neighbours
        random.shuffle(self.__neighbours_addr)
        
        for dst_addr in self.__neighbours_addr:
            ack_no = self.__call_delete_service(
                (self.__host, self.__grpc_port), dst_addr,
                request_id, replica_no, key)

            if ack_no > replica_no:
                replica_no = ack_no # Next replica id to delete

            if ack_no == N_REPLICAS:
                break # All replicas are updated

        return replica_no >= MIN_REPLICAS, ack_no 
    
    """ DELETE Protocol Implementation Ends """


    def run_protocol(self, request_id:int, replica_no:int, event:Events, key:bytes, value:bytes) -> Tuple[bool, object]:
        if event == Events.CREATE or event == Events.CREATE_REDIR:
            success, value = self.create_protocol(
                request_id, replica_no, key, value)

        elif event == Events.READ:
            success, value = self.read_protocol(
                request_id, key)

        elif event == Events.UPDATE:
            success, value = self.update_protocol(
                request_id, replica_no, key, value)
        
        elif event == Events.DELETE:
            success, value = self.delete_protocol(
                request_id, replica_no, key)
            
        return success, value


    def start(self) -> None:
        addr_as_str = lambda h, p : "{}:{}".format(h, p) 
        for addr in self.__neighbours_addr:
            channel = grpc.insecure_channel(addr_as_str(*addr))
            self.__neighbours.update({addr:pirestore_pb2_grpc.PireKeyValueStoreStub(channel)})
        self.__send_greetings()
