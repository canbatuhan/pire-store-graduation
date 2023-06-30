import copy
from typing import Dict
import asyncio
import grpc
import yaml

from pire.modules.cluster      import ClusterHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database     import LocalDatabase
from pire.modules.service      import pirestore_pb2
from pire.modules.service      import pirestore_pb2_grpc
from pire.util.event           import Event

class PireStorage(pirestore_pb2_grpc.PireStoreServicer):
    # Cluster Constants
    MAX_REPLICAS     = int()
    MIN_REPLICAS     = int()

    # Node Constants
    HOST             = str()
    API_PORT         = int()
    GRPC_PORT        = int()
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
        self.HOST             = comm_cfg.get("host")
        self.GRPC_PORT        = comm_cfg.get("grpc_port")
        self.API_PORT         = comm_cfg.get("api_port")
        self.N_WORKERS        = node_cfg.get("n_workers")
        self.MIN_DUMP_TIMEOUT = db_cfg.get("min_poll_time")
        self.MAX_DUMP_TIMEOUT = db_cfg.get("max_poll_time")
        self.ENCODING         = node_cfg.get("encoding")

        # gRPC Service
        self.__store_service = grpc.aio.server()
        pirestore_pb2_grpc.add_PireStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("127.0.0.1:{}".format(self.GRPC_PORT))

        # Cluster Handler
        neighbours           = [(n.get("host"), n.get("port")) for n in neighbour_cfg]
        self.cluster_handler = ClusterHandler(neighbours, self.MAX_REPLICAS, self.MIN_REPLICAS)
        
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
        await self.cluster_handler.greet_protocol_receiver(neigh_addr)
        return pirestore_pb2.Empty()
    
    async def Create(self, request, context):
        try:
            key   = request.payload.key
            value = request.payload.value

            self.create_statemachine_if_not_exists(key)
            self.statemachine_map.get(key).poll(Event.WRITE)
            self.statemachine_map.get(key).trigger(Event.WRITE)

            success = self.database.create(key, value)
            
            if success: # Creates in the local
                request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.GRPC_PORT)])
                request.metadata.replica += 1

                # Creates in the neighbour
                if request.metadata.replica < self.MAX_REPLICAS:
                    _, ack = await self.cluster_handler.create_protocol(request)
                    if ack > request.metadata.replica:
                        request.metadata.replica = ack

            self.statemachine_map.get(key).trigger(Event.DONE)

        except: # State machine polling timeout
            pass
        
        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)
    
    async def Read(self, request, context):
        try:
            key = request.payload.key

            self.create_statemachine_if_not_exists(key)
            self.statemachine_map.get(key).poll(Event.READ)
            self.statemachine_map.get(key).trigger(Event.READ)

            success, value, version = self.database.read(key)
            
            if success: # Reads from the local, validate
                grpcValidate = pirestore_pb2.ValidateProtocolMessage(
                    payload=pirestore_pb2.ValidatePayload(
                    key=key, value=value, version=version))
                
                val_value, val_version = await self.cluster_handler.validate_protocol(grpcValidate)
                
                if version < val_version: # Eventual-consistency !
                    self.database.validate(key, val_value, val_version)
                    value = val_value
        
            else: # Could not read from the local, extend the protocol
                request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.GRPC_PORT)])
                success, value, visited = await self.cluster_handler.read_protocol(request)
                request.metada.visited = visited
            
            self.statemachine_map.get(key).trigger(Event.DONE)
            return pirestore_pb2.ReadAck(
                success = success,
                value   = value,
                visited = request.metada.visited)

        except: # State machine polling timeout
            return pirestore_pb2.ReadAck(
                success = False,
                value   = None,
                visited = request.metadata.visited)

    async def Validate(self, request, context):
        try:
            key         = request.payload.key
            req_value   = request.payload.value
            req_version = request.payload.version

            self.create_statemachine_if_not_exists(key)
            self.statemachine_map.get(key).poll(Event.READ)
            self.statemachine_map.get(key).trigger(Event.READ)

            _, value, version = self.database.read(key)

            # Eventual-consistency !
            if version < req_version or (version == req_version and value != req_value):
                self.database.validate(key, req_value, req_version)
                value = req_value
                version = req_version

            self.statemachine_map.get(key).trigger(Event.DONE)
            return pirestore_pb2.ValidateAck(value=value, version=version)

        except: # State machine polling timeout
            return pirestore_pb2.ValidateAck(
                value   = request.payload.value,
                version = request.payload.version)
    
    async def Update(self, request, context):
        try:
            key   = request.payload.key
            value = request.payload.value

            self.create_statemachine_if_not_exists(key)
            self.statemachine_map.get(key).poll(Event.WRITE)
            self.statemachine_map.get(key).trigger(Event.WRITE)

            success = self.database.update(key, value)
            if success: # Updates in the local
                request.metadata.replica += 1

            request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.GRPC_PORT)])
            if request.metadata.replica < self.MAX_REPLICAS:
                _, ack, visited = await self.cluster_handler.update_protocol(request)
                if ack > request.metadata.replica:
                    request.metadata.replica = ack
                    request.metadata.visited = visited

            self.statemachine_map.get(key).trigger(Event.DONE)

        except: # State machine polling timeout
            pass

        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)
    
    async def Delete(self, request, context):
        try:
            key = request.payload.key

            self.create_statemachine_if_not_exists(key)
            self.statemachine_map.get(key).poll(Event.WRITE)
            self.statemachine_map.get(key).trigger(Event.WRITE)

            success = self.database.delete(key)
            if success: # Updates in the local
                request.metadata.replica += 1

            request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.GRPC_PORT)])
            if request.metadata.replica < self.MAX_REPLICAS:
                _, ack, visited = await self.cluster_handler.delete_protocol(request)
                if ack > request.metadata.replica:
                    request.metadata.replica = ack
                    request.metadata.visited = visited

            self.statemachine_map.get(key).trigger(Event.DONE)

        except: # State machine polling timeout
            pass

        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)

    """ gRPC Service Implementation Ends"""

    async def __dumping_task(self) -> None:
        pair_count = self.database.get_size()
        timeout = self.MIN_DUMP_TIMEOUT
        while True: # Infinite loop
            await asyncio.sleep(timeout)
            try: # Try to dump
                if self.database.get_size() == pair_count:
                    self.database.save()
                    timeout = self.MIN_DUMP_TIMEOUT
                else: # Database is active
                    timeout *= 2
                    if timeout > self.MAX_DUMP_TIMEOUT:
                        timeout = self.MAX_DUMP_TIMEOUT
            except: # Failed to dump
                timeout *= 2
                if timeout > self.MAX_DUMP_TIMEOUT:
                    timeout = self.MAX_DUMP_TIMEOUT
            pair_count = self.database.get_size()

    async def run(self) -> None:
        self.database.start()
        await self.cluster_handler.start(
            self.HOST, self.API_PORT)
        asyncio.gather(
            self.__store_service.start(),
            self.__dumping_task())