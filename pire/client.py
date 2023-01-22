import json

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
        self.__comm_handler = CommunicationHandler(config_paths.get("topology"), self.__id)
        self.__statemachine = ReplicatedStateMachine(config_paths.get("statemachine"))
        self.__database = LocalDatabase(config_paths.get("database"))

    def Read(self, request, context):
        return super().Read(request, context)

    def Prepare(self, request, context):
        return super().Prepare(request, context)

    def Commit(self, request, context):
        return super().Commit(request, context)

    def Rollback(self, request, context):
        return super().Rollback(request, context)