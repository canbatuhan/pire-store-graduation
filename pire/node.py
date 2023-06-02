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
                    request.payload.key.decode(request.payload.encoding),
                    request.payload.val.decode(request.payload.encoding))
            
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
            self.create_statemachine_if_not_exists(request.payload.key)
            self.statemachine_map.get(request.payload.key).poll(Event.READ)
            self.statemachine_map.get(request.payload.key).trigger(Event.READ)

            if request.payload.blind: # A blind read
                success, value = self.database.read(
                    request.payload.key.decode(request.payload.encoding))
                if not success: # Not found in the local database
                    success, value = self.cluster_handler.read_protocol(request)
            
            else: # Not a blind read
                dst_addr = (request.dest.host, request.dest.port)

                # The node is the destination
                if dst_addr == self.cluster_handler.get_address():
                    success, value = self.database.read(
                        request.payload.key.decode(request.payload.encoding))
                else: # Redirect request
                    success, value = self.cluster_handler.read_protocol(request)

            self.statemachine_map.get(request.payload.key).trigger(Event.DONE)
            
        except PollingTimeoutException:
            pass # Could not trigger state machine

        # Send acknowledgment
        return pirestore_pb2.ReadAck(success=success, val=value, encoding=request.payload.encoding)
    
    def Update(self, request, context):
        try: # Try to execute
            self.create_statemachine_if_not_exists(request.payload.key)
            self.statemachine_map.get(request.payload.key).poll(Event.WRITE)
            self.statemachine_map.get(request.payload.key).trigger(Event.WRITE)

            if request.payload.blind: # A blind update
                success = self.database.update(
                    request.payload.key.decode(request.payload.encoding),
                    request.payload.val.decode(request.payload.encoding))
                
                if success: # Updated locally
                    request.payload.replica += 1
                
                # Extend the protocol
                if request.payload.replica != self.MAX_REPLICAS:
                    _, ack = self.cluster_handler.update_protocol(request)
                    
                    # Some pairs are updated
                    if ack > request.payload.replica:
                        request.payload.replica = ack # inplace!
                
            else: # Not a blind update
                is_destination = False
                for each in request.dests.vals:
                    dst_addr = (each.host, each.port) 
                    if dst_addr == self.cluster_handler.get_address():
                        is_destination = True
                        break
                
                # The node is the destination
                if is_destination:
                    success = self.database.update(
                        request.payload.key.decode(request.payload.encoding),
                        request.payload.val.decode(request.payload.encoding))

                else: # Redirect request
                    _, ack = self.cluster_handler.update_protocol(request)

                    # Some pairs are updated
                    if ack > request.payload.replica:
                        request.payload.replica = ack # inplace!

            self.statemachine_map.get(request.payload.key).trigger(Event.DONE)

        except PollingTimeoutException:
            pass # Could not trigger state machine

        # Send acknowledgment
        return pirestore_pb2.WriteAck(replica=request.payload.replica)
    
    def Delete(self, request, context):
        return super().Delete(request, context)

    """ gRPC Service Implementation Ends"""


                