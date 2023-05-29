import copy
from typing import Dict
import grpc
import yaml
from concurrent import futures

from pire.modules.cluster      import NULL_DESTINATION_HOST, ClusterHandler
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
    DISCOVER_PERIOD  = float()

    # Node Constants
    MIN_DUMP_TIMEOUT = float()
    MAX_DUMP_TIMEOUT = float()
    ENCODING         = str()

    def __init__(self, config_file_path:str) -> None:
        with open(config_file_path, "r") as file:
            cfg = dict(yaml.load(file))

        # Configurations
        cluster_cfg    = dict(cfg.get("cluster"))
        node_cfg       = dict(cfg.get("node"))
        neighbour_cfg  = dict(node_cfg.get("neighbours"))
        sm_cfg         = dict(node_cfg.get("statemachine"))
        db_cfg         = dict(node_cfg.get("database"))
        comm_cfg       = dict(node_cfg.get("communication"))

        # Cluster Configurations
        self.MAX_REPLICAS    = cluster_cfg.get("max_replicas")
        self.MIN_REPLICAS    = cluster_cfg.get("min_replicas")
        self.DISCOVER_PERIOD = cluster_cfg.get("discover_period")

        # Node Configurations
        self.N_WORKERS        = node_cfg.get("n_workers")
        self.MIN_DUMP_TIMEOUT = db_cfg.get("min_poll_time")
        self.MAX_DUMP_TIMEOUT = db_cfg.get("max_poll_time")
        self.ENCODING         = node_cfg.get("encoding")

        # gRPC Service
        self.__store_service = grpc.server(futures.ThreadPoolExecutor(max_workers=self.N_WORKERS))
        
        # Cluster Handler
        neighbours           = [(n.get("host"), n.get("port")) for n in neighbour_cfg]
        self.cluster_handler = ClusterHandler(comm_cfg.get("host"), comm_cfg.get("grpc_port"), neighbours, self.MAX_REPLICAS, self.MIN_REPLICAS)
        
        # Replicated State Machine
        self.sample_statemachine = ReplicatedStateMachine(sm_cfg.get("min_poll_time"), sm_cfg.get("max_poll_time"))
        self.statemachine_map:Dict[bytes,ReplicatedStateMachine] = dict()
        
        # Local Database
        self.database = LocalDatabase(db_cfg("path"))

    def create_statemachine_if_not_exists(self, key:bytes) -> None:
        if self.statemachine_map.get(key) == None:
            statemachine = copy.deepcopy(self.sample_statemachine)
            statemachine.start()
            self.statemachine_map.update({key: statemachine})

    """ gRPC Service Implementation Starts """

    def Greet(self, request, context):
        neigh_addr = (request.sender.host, request.sender.port)
        self.cluster_handler.greet_protocol_receiver(neigh_addr)
        return pirestore_pb2.Empty()
    
    def Discover(self, request, context):
        neigh_addr = (request.sender.host, request.sender.port)
        self.cluster_handler.discover_protocol_receiver(
            neigh_addr, request.created.vals, request.deleted.vals)
        return pirestore_pb2.Empty()
    
    def Create(self, request, context):
        try: # Try to execute
            self.create_statemachine_if_not_exists(request.payload.key)
            self.statemachine_map.get(request.payload.key).poll(Event.WRITE)
            self.statemachine_map.get(request.payload.key).trigger(Event.WRITE)

            # Received 'CREATE' request
            if request.direct:
                success = self.database.create(
                    request.payload.key.decode(request.encoding),
                    request.payload.val.decode(request.encoding))
            
                if success: # Created locally
                    request.payload.replica += 1 # inplace!
                    self.cluster_handler.add_to_created(request.payload.key)

            else: # Received 'CREATE_REDIRECT' request
                _, ack = self.cluster_handler.create_protocol(request)

                # Some pairs are created
                if ack > request.payload.replica:
                    request.payload.replica = ack # inplace!

            self.statemachine_map.get(request.payload.key).trigger(Event.DONE)

        except PollingTimeoutException:
            pass # Could not trigger state machine
        
        # Send acknowledgment
        return pirestore_pb2.WriteAck(replica=request.payload.replica)
    
    def Read(self, request, context):
        success = False
        value   = None

        try: # Try to execute

            # Handle the request
            self.create_statemachine_if_not_exists(request.key)
            self.statemachine_map.get(request.key).poll(Event.READ)
            self.statemachine_map.get(request.key).trigger(Event.READ)

            # Read from the local database
            if (request.dest.host, request.dest.port) == self.cluster_handler.get_address():
                success, value = self.database.read(request.key.decode(request.encoding))

            elif request.dest.host == NULL_DESTINATION_HOST: # Null destination
                success, value = self.database.read(request.key.decode(request.encoding))
                if not success: # Could not read from the local database
                    success, value = self.cluster_handler.read_protocol(request)
            
            else: # Redirect request
                success, value = self.cluster_handler.read_protocol(request)

            self.statemachine_map.get(request.key).trigger(Event.DONE)

        except PollingTimeoutException:
            pass # Could not trigger state machine

        return pirestore_pb2.ReadAck(success=success, val=value, encoding=request.encoding)
    
    def Update(self, request, context):
        return super().Update(request, context)
    
    def Delete(self, request, context):
        return super().Delete(request, context)

    """ gRPC Service Implementation Ends"""


                