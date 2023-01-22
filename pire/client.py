import grpc
import json
from threading import Thread
from concurrent import futures

from pire.modules.server import pirestore_pb2
from pire.modules.server import pirestore_pb2_grpc
from pire.modules.communication import CommunicationHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database import LocalDatabase
from pire.util.constants import CLIENT_CONFIG_PATH

class PireClient(pirestore_pb2_grpc.PireKeyValueStoreServicer):

    def __comm_handler_test(self) -> None:
        self.__comm_handler.start()

    def __statemachine_test(self) -> None:
        self.__statemachine.start()

    def __db_test(self) -> None:
        self.__database.start()

    def test_components(self) -> None:
        self.__comm_handler_test()
        self.__statemachine_test()
        self.__db_test()

    def __init__(self, client_id:str) -> None:
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))
        self.__id = client_id
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        self.__comm_handler = CommunicationHandler(config_paths.get("topology"), self.__id)
        self.__statemachine = ReplicatedStateMachine(config_paths.get("statemachine"))
        self.__database = LocalDatabase(config_paths.get("database"))
        
    def Greet(self, request, context):
        return super().Greet(request, context)

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
        server_host, server_port = self.__comm_handler.get_address()
        pirestore_pb2_grpc.add_PireKeyValueStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("{}:{}".format(server_host, server_port))
        self.__store_service.wait_for_termination()

    def user_thread(self) -> None:
        while True:
            connection, addr = self.__comm_handler.establish_connection()
            request = self.__comm_handler.receive_request(connection)
            ack = self.__comm_handler.handle_request(request)
            self.__comm_handler.send_ack(connection, addr, ack)

    def run(self):
        Thread(target=self.grpc_thread).start()
        Thread(target=self.user_thread).start()