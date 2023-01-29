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

from pire.util.constants import CLIENT_CONFIG_PATH
from pire.util.enums import Events
from pire.util.exceptions import ConnectionLostException, PollingTimeoutException


class PireClient(pirestore_pb2_grpc.PireKeyValueStoreServicer):

    def __init__(self, client_id:str) -> None:
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))
        self.__id = client_id
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.__comm_handler = CommunicationHandler(self.__id, config_paths.get("topology"))
        self.__statemachine = ReplicatedStateMachine(self.__id, config_paths.get("statemachine"))
        self.__database = LocalDatabase(self.__id)
        
    def Greet(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address() 
        dst_addr = (request.destination.host, request.destination.port)
        if grpc_addr == dst_addr:
            self.__comm_handler.accept_greeting(dst_addr)
            return pirestore_pb2.Ack(
                success=True,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.destination.host, port=request.destination.port))  
        else: # Destination address is different
            return pirestore_pb2.Ack(
                success=False,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.destination.host, port=request.destination.port))

    def Create(self, request, context):
        return super().Create(request, context)

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
        cluster_handler = self.__comm_handler.cluster_handler
        while True:
            connection, addr = user_handler.establish_connection()
            while True: # Until someone exits
                try: # Handle user requests
                    request = user_handler.receive_request(connection, addr)
                    if request.lower() == "exit":
                        user_handler.close_connection(connection, addr)
                        break

                    event, key, value = user_handler.parse_request(request)
                    self.__statemachine.poll(event)

                    self.__statemachine.trigger(event)
                    ack = cluster_handler.execute(event, key, value)

                    user_handler.send_ack(connection, addr, ack)
                    self.__statemachine.trigger(Events.DONE)

                except PollingTimeoutException: # Try to receive/close
                    user_handler.close_connection(connection, addr)

                except ConnectionLostException:
                    pass

    def run(self):
        Thread(target=self.__grpc_thread).start()
        Thread(target=self.__user_thread).start()