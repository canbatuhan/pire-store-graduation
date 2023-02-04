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

from pire.util.constants import CLIENT_CONFIG_PATH, ENCODING, MAX_ID, N_REPLICAS
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException, InvalidRequestType, PollingTimeoutException


class PireClient(pirestore_pb2_grpc.PireKeyValueStoreServicer):

    def __init__(self, client_id:str) -> None:
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))
        self.__id = client_id
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.__comm_handler = CommunicationHandler(self.__id, config_paths.get("topology"))
        self.__statemachine = ReplicatedStateMachine(self.__id, config_paths.get("statemachine"))
        self.__database = LocalDatabase(self.__id)
        self.__history:List[int] = list()
        
    def Greet(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address() 
        dst_addr = (request.destination.host, request.destination.port)
        src_addr = (request.source.host, request.source.port)

        if grpc_addr == dst_addr:
            self.__comm_handler.cluster_handler.accept_greeting(src_addr)
            return pirestore_pb2.Ack(
                success=True,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))  

        else: # Destination address is different
            return pirestore_pb2.Ack(
                success=False,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))

    def Create(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        success = False

        if request.id not in self.__history:
            self.__history.append(request.id)

            if request.command == Events.CREATE.value:
                self.__statemachine.poll(Events.CREATE)
                self.__statemachine.trigger(Events.CREATE)              
                success = self.__database.create(
                    request.key.decode(request.encoding),
                    request.value.decode(request.encoding))
                self.__statemachine.trigger(Events.DONE)

            elif request.command == Events.CREATE_REDIR.value:
                self.__statemachine.poll(Events.CREATE)
                self.__statemachine.trigger(Events.CREATE)  
                success = self.__comm_handler.cluster_handler.run_replication_protocol(request)
                self.__statemachine.trigger(Events.DONE)

        return pirestore_pb2.Ack(
                success=success,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))

    def Read(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address()
        read_success = False
        read_value = None

        if request.id not in self.__history:
            self.__history.append(request.id)

            if request.command == Events.READ.value:
                self.__statemachine.poll(Events.READ)
                self.__statemachine.trigger(Events.READ)
                read_success, read_value = self.__database.read(
                    request.key.decode(request.encoding))

                if read_success: # Found in local
                    read_value = read_value.encode(ENCODING)

                else: # Can not found in local
                    read_success, read_value = self.__comm_handler.cluster_handler.run_main_protocol(
                        request.id, None, Events.READ, request.key, request.value)
                
                self.__statemachine.trigger(Events.DONE)
                    
        return pirestore_pb2.Response(
            success=read_success,
            value=read_value,
            encoding=ENCODING,
            source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
            destination=pirestore_pb2.Address(host=request.source.host, port=request.source.port))

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
        success, value = cluster_handler.run_main_protocol(random_id, replica_no, event, key, value)
        return success, value

    def start(self):
        self.__comm_handler.start()
        self.__statemachine.start()
        self.__database.start()

    def __grpc_thread(self) -> None:
        grpc_addr, _ = self.__comm_handler.get_address()
        pirestore_pb2_grpc.add_PireKeyValueStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("{}:{}".format(*grpc_addr))
        self.__store_service.start()
        self.__store_service.wait_for_termination()

    def __user_thread(self) -> None:
        user_handler = self.__comm_handler.user_request_handler

        while True:
            connection, addr = user_handler.establish_connection()
            while True: # Until someone exits
                try: # Handle user requests
                    request = user_handler.receive_request(connection, addr)
                    if request.lower() == b"exit":
                        user_handler.close_connection(connection, addr)
                        break

                    event, key, value = user_handler.parse_request(request)
                    self.__statemachine.poll(event)

                    self.__statemachine.trigger(event)
                    ack, read_value = self.__handle_request(event, key, value)

                    user_handler.send_ack(connection, addr, ack, read_value)
                    self.__statemachine.trigger(Events.DONE)

                except PollingTimeoutException: # Try to receive/close
                    user_handler.close_connection(connection, addr)

                except InvalidRequestType:
                    user_handler.send_ack(connection, addr, "Invalid request type")
                        
                except ConnectionLostException:
                    break

    def run(self):
        Thread(target=self.__grpc_thread).start()
        Thread(target=self.__user_thread).start()