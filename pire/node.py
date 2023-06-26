import copy
from typing import Dict, List
import asyncio
import grpc
import yaml
from concurrent import futures

from pire.modules.cluster      import ClusterHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database     import LocalDatabase
from pire.modules.service      import pirestore_pb2
from pire.modules.service      import pirestore_pb2_grpc
from pire.util.exception       import PollingTimeoutException
from pire.util.event           import Event

class PireNode(pirestore_pb2_grpc.PireStoreServicer):
    # Cluster Constants
    MAX_REPLICAS     = int()
    MIN_REPLICAS     = int()

    # Node Constants
    MIN_DUMP_TIMEOUT = float()
    MAX_DUMP_TIMEOUT = float()
    ENCODING         = str()

    def __init__(self, config_file_path:str) -> None:
        with open(config_file_path, "r") as file:
            cfg = dict(yaml.load(file))

        # Configurations
        cluster_cfg    = cfg.get("cluster")
        node_cfg       = cfg.get("node")
        neighbour_cfg  = node_cfg.get("neighbours")
        sm_cfg         = node_cfg.get("statemachine")
        db_cfg         = node_cfg.get("database")
        comm_cfg       = node_cfg.get("communication")

        # Cluster Configurations
        self.MAX_REPLICAS    = cluster_cfg.get("max_replicas")
        self.MIN_REPLICAS    = cluster_cfg.get("min_replicas")

        # Node Configurations
        self.N_WORKERS        = node_cfg.get("n_workers")
        self.MIN_DUMP_TIMEOUT = db_cfg.get("min_poll_time")
        self.MAX_DUMP_TIMEOUT = db_cfg.get("max_poll_time")
        self.ENCODING         = node_cfg.get("encoding")

        # gRPC Service
        self.__store_service = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=self.N_WORKERS))
        pirestore_pb2_grpc.add_PireStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("0.0.0.0:{}".format(comm_cfg.get("grpc_port")))

        # Cluster Handler
        neighbours           = [(n.get("host"), n.get("port")) for n in neighbour_cfg]
        self.cluster_handler = ClusterHandler(comm_cfg.get("host"), comm_cfg.get("grpc_port"), neighbours, self.MAX_REPLICAS, self.MIN_REPLICAS)
        
        # Replicated State Machine
        self.sample_statemachine = ReplicatedStateMachine(sm_cfg.get("min_poll_time"), sm_cfg.get("max_poll_time"))
        self.statemachine_map:Dict[bytes,ReplicatedStateMachine] = dict()
        
        # Local Database
        self.database = LocalDatabase(db_cfg.get("path"))

    def create_statemachine_if_not_exists(self, key:bytes) -> None:
        if self.statemachine_map.get(key) == None:
            statemachine = copy.deepcopy(self.sample_statemachine)
            statemachine.start()
            self.statemachine_map.update({key: statemachine})

    """ gRPC Service Implementation Starts """

    async def Greet(self, request, context):
        neigh_addr = (request.sender.host, request.sender.port)
        self.cluster_handler.greet_protocol_receiver(neigh_addr)
        return pirestore_pb2.Empty()
    
    async def Create(self, request, context):
        try:
            host, port = self.cluster_handler.get_address()
            key = request.payload.key
            val = request.payload.val

            self.statemachine_map.get(key).poll(Event.WRITE)
            self.statemachine_map.get(key).trigger(Event.WRITE)

            success = self.database.create(
                key.encode(self.ENCODING),
                val.encode(self.ENCODING))
            
            if success: # Creates in local
                request.metadata.visited.extend([pirestore_pb2.Address(host=host, port=port)])
                request.metadata.replica += 1
            
            if request.metadata.replica < self.MAX_REPLICAS:
                _, ack = self.cluster_handler.create_protocol(request)
                if ack > request.metadata.replica:
                    request.metadata.replica = ack

            self.statemachine_map.get(key).trigger(Event.DONE)

        except: # State machine polling timeout
            pass
        
        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)
    
    async def Read(self, request, context):
        pass
    
    async def Update(self, request, context):
        pass
    
    async def Delete(self, request, context):
        pass

    """ gRPC Service Implementation Ends"""


                