import socket
import time
from typing import List, Tuple
import grpc
import json
import random
from threading import Thread
from concurrent import futures

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.modules.communication.handler import CommunicationHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database import LocalDatabase

from pire.util.constants import CLIENT_CONFIG_PATH, ENCODING, MAX_DUMP_TIMEOUT, MAX_ID, MIN_DUMP_TIMEOUT, N_SERVICER, N_HANDLER
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException, InvalidRequestType, PollingTimeoutException


class PireClient(pirestore_pb2_grpc.PireKeyValueStoreServicer):

    def __init__(self, client_id:str) -> None:
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))
        self.__id = client_id
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=N_SERVICER))
        self.__comm_handler = CommunicationHandler(self.__id, config_paths.get("topology"))
        self.__statemachine = ReplicatedStateMachine(config_paths.get("statemachine"))
        self.__database = LocalDatabase()
        self.__history:List[int] = list()
    

    """ gRPC Service Implementations Start """

    def Greet(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address() 
        dst_addr = (request.destination.host, request.destination.port)
        src_addr = (request.source.host, request.source.port)

        if grpc_addr == dst_addr:
            self.__comm_handler.cluster_handler.accept_greeting(src_addr)
            return pirestore_pb2.WriteAck(
                success=True,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))  

        else: # Destination address is different
            return pirestore_pb2.WriteAck(
                success=False,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))

    def Create(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        replica_no = request.replica_no

        try:
            # First time processing
            if request.id not in self.__history:
                self.__history.append(request.id)

                # Trigger the state machine
                self.__statemachine.poll(Events.CREATE)
                self.__statemachine.trigger(Events.CREATE)

                # Try to create in local
                if request.command == Events.CREATE.value:           
                    success = self.__database.create(
                        request.key.decode(request.encoding),
                        request.value.decode(request.encoding))
                    
                    if success: # Created locally
                        replica_no += 1 # Increment replica nubmer

                # Redirect CREATE message, run protocol for CREATE
                elif request.command == Events.CREATE_REDIR.value:  
                    _, ack_no = self.__comm_handler.cluster_handler.create_protocol(
                        request.id, request.replica_no, request.key, request.value)
                    
                    if ack_no > replica_no: # Some pairs are created
                        replica_no = ack_no

                self.__statemachine.trigger(Events.DONE)
        
        except PollingTimeoutException:
            pass
        
        # Send acknowledgement
        return pirestore_pb2.WriteAck(
                ack_no=replica_no, # Next replica id to create
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))

    def Read(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        read_success = False
        read_value = None

        try:
            # First time processing
            if request.id not in self.__history:
                self.__history.append(request.id)

                # Trigger the state machine
                self.__statemachine.poll(Events.READ)
                self.__statemachine.trigger(Events.READ)
                read_success, read_value = self.__database.read(
                    request.key.decode(request.encoding))

                if read_success: # Found in local
                    read_value = read_value.encode(ENCODING)

                else: # Can not found in local, run protocol for READ
                    read_success, read_value = self.__comm_handler.cluster_handler.read_protocol(
                        request.id, request.key)
                
                self.__statemachine.trigger(Events.DONE)

        except PollingTimeoutException:
            pass
        
        # Send response
        return pirestore_pb2.ReadAck(
            success=read_success,
            value=read_value,
            encoding=ENCODING,
            source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
            destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))
    
    def Update(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        replica_no = request.replica_no

        try:
            # First time processing
            if request.id not in self.__history:
                self.__history.append(request.id)

                # Trigger the state machine
                self.__statemachine.poll(Events.UPDATE)
                self.__statemachine.trigger(Events.UPDATE)
                success = self.__database.update(
                    request.key.decode(request.encoding),
                    request.value.decode(request.encoding))
                
                if success: # Updated locally
                    replica_no += 1
                
                # Run protocol for UPDATE
                _, ack_no = self.__comm_handler.cluster_handler.update_protocol(
                    request.id, replica_no, request.key, request.value)
                
                if ack_no > replica_no: # Some pairs are updated
                    replica_no = ack_no
                
                self.__statemachine.trigger(Events.DONE)
        
        except PollingTimeoutException:
            pass

        # Send response
        return pirestore_pb2.WriteAck(
                ack_no=replica_no, # Next replica id to update!
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))
    
    def Delete(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        replica_no = request.replica_no

        try:
            # First time processing
            if request.id not in self.__history:
                self.__history.append(request.id)

                # Trigger the state machine
                self.__statemachine.poll(Events.DELETE)
                self.__statemachine.trigger(Events.DELETE)
                success = self.__database.delete(
                    request.key.decode(request.encoding))
                
                if success: # Deleted locally
                    replica_no += 1
                
                # Run protocol for DELETE
                _, ack_no = self.__comm_handler.cluster_handler.delete_protocol(
                    request.id, replica_no, request.key)
                
                if ack_no > replica_no: # Some pairs are Deleted
                    replica_no = ack_no
                
                self.__statemachine.trigger(Events.DONE)

        except PollingTimeoutException:
            pass
        
        # Send response
        return pirestore_pb2.WriteAck(
                ack_no=replica_no, # Next replica id to update!
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))
    
    """ gRPC Service Implementations End """


    """ Helper Functions Start """
    
    def __grpc_thread(self) -> None:
        grpc_addr, _ = self.__comm_handler.get_address()
        pirestore_pb2_grpc.add_PireKeyValueStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("0.0.0.0:{}".format(grpc_addr[1]))
        self.__store_service.start()
        self.__store_service.wait_for_termination()

    def __database_thread(self) -> None:
        pair_count = self.__database.get_size()
        timeout = MIN_DUMP_TIMEOUT

        while True: # Infinite loop
            time.sleep(timeout)
            
            try: # Try to dump
                if self.__database.get_size() == pair_count:
                    self.__database.save()
                    timeout = MIN_DUMP_TIMEOUT

                else: # Database is active
                    timeout *= 2
                    if timeout > MAX_DUMP_TIMEOUT:
                        timeout = MAX_DUMP_TIMEOUT

            except: # Failed to dump
                timeout *= 2
                if timeout > MAX_DUMP_TIMEOUT:
                    timeout = MAX_DUMP_TIMEOUT
    
            pair_count = self.__database.get_size()

    def __handle_request(self, event:Events, key:bytes, value:bytes) -> Tuple[bool, bytes]:
        cluster_handler = self.__comm_handler.cluster_handler
        replica_no = 0

        # Read operation
        if event == Events.READ:
            success, read_value = self.__database.read(
                key.decode(ENCODING))
            if success: # If key-value pair is found
                return True, read_value

        # Write operations
        if event == Events.CREATE:
            success = self.__database.create(
                key.decode(ENCODING), value.decode(ENCODING))

        elif event == Events.UPDATE:
            success = self.__database.update(
                key.decode(ENCODING), value.decode(ENCODING))

        elif event == Events.DELETE:
            success = self.__database.delete(
                key.decode(ENCODING))

        if success: # Successful write operations
            replica_no += 1

        # Run corresponding protocol
        random_id = random.choice(range(0, int(MAX_ID)))
        self.__history.append(random_id)
        
        success, read_value = cluster_handler.run_protocol(
            random_id, replica_no, event, key, value)
        
        return success, read_value
    
    def __request_handler_callback(self, connection:socket.socket, addr:Tuple[str,int]):
        user_handler = self.__comm_handler.user_request_handler
        
        try: # Handle user requests
            request = user_handler.receive_request(connection, addr)

            # Parse requests: create(...), read(...), update(...), delete(...)
            event, key, value = user_handler.parse_request(request)
            self.__statemachine.poll(event)

            # Trigger transitions
            self.__statemachine.trigger(event)
            ack, read_value = self.__handle_request(event, key, value)

            # Send acknowledgement to user
            user_handler.send_ack(connection, addr, ack, read_value)
            self.__statemachine.trigger(Events.DONE)
        
        except PollingTimeoutException: # Try to receive/close
            user_handler.send_ack(connection, addr, False, 0)

        except InvalidRequestType: # Try to receive/close
            pass

        except ConnectionLostException:
            pass

    def __user_thread(self) -> None:
        user_handler = self.__comm_handler.user_request_handler
        request_handler_pool = futures.ThreadPoolExecutor(max_workers=N_HANDLER)

        while True: # Infinite loop
            connection, addr = user_handler.establish_connection()
            request_handler_pool.submit(self.__request_handler_callback, connection, addr)
            user_handler.close_connection(connection, addr)
    
    """ Helper Functions End """


    def start(self):
        self.__comm_handler.start()
        self.__statemachine.start()
        self.__database.start()

    def run(self):
        Thread(target=self.__grpc_thread).start()
        Thread(target=self.__database_thread).start()
        Thread(target=self.__user_thread).start()