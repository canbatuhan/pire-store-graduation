import copy
from typing import Dict
import asyncio
import grpc

from pire.modules.cluster      import ClusterHandler
from pire.modules.statemachine import ReplicatedStateMachine
from pire.modules.database     import LocalDatabase
from pire.modules.service      import pirestore_pb2
from pire.modules.service      import pirestore_pb2_grpc
from pire.util.event           import Event

class PireStore(pirestore_pb2_grpc.PireStoreServicer):
    # Cluster Constants
    MAX_REPLICAS     = int()
    MIN_REPLICAS     = int()

    # Node Constants
    HOST             = str()
    PORT             = int()
    MIN_DUMP_TIMEOUT = float()
    MAX_DUMP_TIMEOUT = float()
    ENCODING         = str()

    def __init__(self, config:dict) -> None: 
        # Sub-configurations
        replica_cfg   = config.get("replication")
        neighbour_cfg = config.get("neighbours")
        sm_cfg        = config.get("statemachine")
        db_cfg        = config.get("database")

        # Replica Criteria Configuration
        self.MAX_REPLICAS = replica_cfg.get("max_replicas")
        self.MIN_REPLICAS = replica_cfg.get("min_replicas")

        # Neighbour Configurations
        neighbours = [(n.get("host"), n.get("port")) for n in neighbour_cfg]

        # Communication Configurations
        self.HOST = config.get("host")
        self.PORT = config.get("port")

        # State Machine Configurations
        sm_min_poll_time = sm_cfg.get("min_poll_time")
        sm_max_poll_time = sm_cfg.get("max_poll_time")

        # Database Configurations
        database_file_path    = db_cfg.get("path")
        self.MIN_DUMP_TIMEOUT = db_cfg.get("min_poll_time")
        self.MAX_DUMP_TIMEOUT = db_cfg.get("max_poll_time")

        # gRPC Service
        self.__store_service = grpc.aio.server()
        pirestore_pb2_grpc.add_PireStoreServicer_to_server(self, self.__store_service)
        self.__store_service.add_insecure_port("0.0.0.0:{}".format(self.PORT))

        # Cluster Handler
        self.cluster_handler = ClusterHandler(neighbours, self.MAX_REPLICAS, self.MIN_REPLICAS)
        
        # Replicated State Machine
        self.sample_statemachine = ReplicatedStateMachine(sm_min_poll_time, sm_max_poll_time)
        self.statemachine_map:Dict[str,ReplicatedStateMachine] = dict()
        
        # Local Database
        self.database = LocalDatabase(database_file_path)

    def get_state_machine(self, key:str) -> ReplicatedStateMachine:
        statemachine = self.statemachine_map.get(key)
        if statemachine == None: # Create if not exists
            statemachine = copy.deepcopy(self.sample_statemachine)
            statemachine.start()
            self.statemachine_map.update({key: statemachine})
        return statemachine

    """ gRPC Service Implementation Starts """

    async def Greet(self, request, context):
        neigh_addr = (request.sender.host, request.sender.port)
        await self.cluster_handler.greet_protocol_receiver(neigh_addr)
        return pirestore_pb2.Empty()
    
    async def Create(self, request, context):
        try:
            key   = request.payload.key
            value = request.payload.value

            statemachine = self.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            success = self.database.create(key, value)
            
            if success: # Creates in the local
                request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.PORT)])
                request.metadata.replica += 1

                # Creates in the neighbour
                if request.metadata.replica < self.MAX_REPLICAS:
                    _, ack = await self.cluster_handler.create_protocol(request)
                    if ack > request.metadata.replica:
                        request.metadata.replica = ack

            statemachine.trigger(Event.DONE)

        except: # State machine polling timeout
            pass
        
        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)
    
    async def Read(self, request, context):
        try:
            key = request.payload.key

            statemachine = self.get_state_machine(key)
            statemachine.poll(Event.READ)
            statemachine.trigger(Event.READ)

            success, value, version = self.database.read(key)
            
            if success: # Reads from the local, validate
                grpc_validate = pirestore_pb2.ValidateProtocolMessage(
                    payload = pirestore_pb2.ValidatePayload(
                        key = key, value = value, version = version))
                
                val_value, val_version = await self.cluster_handler.validate_protocol(grpc_validate)
                
                if version < val_version: # Eventual-consistency !
                    self.database.validate(key, val_value, val_version)
                    value = val_value
        
            else: # Could not read from the local, extend the protocol
                request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.PORT)])
                success, value, visited = await self.cluster_handler.read_protocol(request)
                del request.metadata.visited[:]
                request.metada.visited.extend(visited)
            
            statemachine.trigger(Event.DONE)
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

            statemachine = self.get_state_machine(key)
            statemachine.poll(Event.READ)
            statemachine.trigger(Event.READ)

            _, value, version = self.database.read(key)

            # Eventual-consistency !
            if version < req_version or (version == req_version and value != req_value):
                self.database.validate(key, req_value, req_version)
                value   = req_value
                version = req_version

            statemachine.trigger(Event.DONE)
            return pirestore_pb2.ValidateAck(value=value, version=version)

        except: # State machine polling timeout
            return pirestore_pb2.ValidateAck(
                value   = request.payload.value,
                version = request.payload.version)
    
    async def Update(self, request, context):
        try:
            key   = request.payload.key
            value = request.payload.value

            statemachine = self.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            success = self.database.update(key, value)
            if success: # Updates in the local
                request.metadata.replica += 1

            else: # Failed in the local
                request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.PORT)])
                if request.metadata.replica < self.MAX_REPLICAS:
                    ack, visited = await self.cluster_handler.update_protocol(request)
                    if ack: # Updated in the neighbour
                        request.metadata.replica = ack
                        del request.metadata.visited[:]
                        request.metada.visited.extend(visited)

            statemachine.trigger(Event.DONE)

        except: # State machine polling timeout
            pass

        return pirestore_pb2.WriteAck(
                ack     = request.metadata.replica,
                visited = request.metadata.visited)
    
    async def Delete(self, request, context):
        try:
            key = request.payload.key

            statemachine = self.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            success = self.database.delete(key)
            if success: # Updates in the local
                request.metadata.replica += 1

            request.metadata.visited.extend([pirestore_pb2.Address(host=self.HOST, port=self.PORT)])
            if request.metadata.replica < self.MAX_REPLICAS:
                _, ack, visited = await self.cluster_handler.delete_protocol(request)
                if ack > request.metadata.replica:
                    request.metadata.replica = ack
                    del request.metadata.visited[:]
                    request.metada.visited.extend(visited)

            statemachine.trigger(Event.DONE)

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
            except Exception as exception: # Failed to dump
                print(exception.with_traceback(None))
                timeout *= 2
                if timeout > self.MAX_DUMP_TIMEOUT:
                    timeout = self.MAX_DUMP_TIMEOUT
            pair_count = self.database.get_size()

    async def __grpc_server_task(self) -> None:
        await self.__store_service.start()
        await self.__store_service.wait_for_termination()

    async def run(self) -> None:
        self.database.start()
        await self.cluster_handler.start(
            self.HOST, self.PORT)
        await asyncio.gather(
            self.__grpc_server_task(),
            self.__dumping_task())