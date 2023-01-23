import grpc
import json
from threading import Thread
from concurrent import futures

from pire.modules.service import pirestore_pb2
from pire.modules.service import pirestore_pb2_grpc
from pire.modules.communication import CommunicationHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database import LocalDatabase
from pire.util.constants import CLIENT_CONFIG_PATH

class PireClient(pirestore_pb2_grpc.PireKeyValueStoreServicer):

    def __init__(self, client_id:str) -> None:
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))
        self.__id = client_id
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.__comm_handler = CommunicationHandler(config_paths.get("topology"), self.__id)
        self.__statemachine = ReplicatedStateMachine(config_paths.get("statemachine"))
        self.__database = LocalDatabase(config_paths.get("database"))
        
    def Greet(self, request, context):
        grpc_addr, _ = self.__comm_handler.get_address() 
        dst_addr = (request.destination.host, request.destination.port)
        if grpc_addr == dst_addr:
            return pirestore_pb2.Ack(
                success=True,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.destination.host, port=request.destination.port))  
        else: # Destination address is different
            return pirestore_pb2.Ack(
                success=False,
                source=pirestore_pb2.Address(host=grpc_addr[0], port=grpc_addr[1]),
                destination=pirestore_pb2.Address(host=request.destination.host, port=request.destination.port))

    def Read(self, request, context):
        return super().Read(request, context)

    def Prepare(self, request, context):
        return super().Prepare(request, context)

    def Commit(self, request, context):
        return super().Commit(request, context)

    def Rollback(self, request, context):
        return super().Rollback(request, context)

    def start(self):
        self.__comm_handler.start()
        self.__statemachine.start()
        self.__database.start()

    def grpc_thread(self) -> None:
        grpc_addr, _ = self.__comm_handler.get_address()
        pirestore_pb2_grpc.add_PireKeyValueStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("{}:{}".format(*grpc_addr))
        self.__store_service.start()
        self.__store_service.wait_for_termination()

    def user_thread(self) -> None:
        pass

    def run(self):
        Thread(target=self.grpc_thread).start()
        Thread(target=self.user_thread).start()