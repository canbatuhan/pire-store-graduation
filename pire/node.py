from typing import List
import yaml
from quart import Quart, request, Response

from pire.store           import PireStore
from pire.modules.service import pirestore_pb2
from pire.util.event      import Event

def __address_message(host:str, port:int) -> pirestore_pb2.Address:
    return pirestore_pb2.Address(
        host = host,
        port = port
    )

def __write_message(key:object, value:object, replica:int, visited:List[pirestore_pb2.Address]) -> pirestore_pb2.WriteProtocolMessage:
    return pirestore_pb2.WriteProtocolMessage(
        payload = pirestore_pb2.WritePayload(
            key   = key,
            value = value
        ),
        metadata = pirestore_pb2.WriteMetadata(
            replica = replica,
            visited = visited
        )
    )

def __read_message(key:object, visited:List[pirestore_pb2.Address]) -> pirestore_pb2.ReadProtocolMessage:
    return pirestore_pb2.ReadProtocolMessage(
        payload = pirestore_pb2.ReadPayload(
            key = key
        ),
        metadata = pirestore_pb2.ReadMetadata(
            visited = visited
        )
    )

def __validate_message(key:object, value:object, version:int) -> pirestore_pb2.ValidateProtocolMessage:
    return pirestore_pb2.ValidateProtocolMessage(
        payload = pirestore_pb2.ValidatePayload(
            key     = key,
            value   = value,
            version = version
        )
    )

class PireNode:
    SERVER:Quart    = Quart(__name__)
    STORE:PireStore = None # pire-store

    def __init__(self, config_path:str=None) -> None:
        with open(config_path, "r") as file:
            cfg = dict(yaml.load(file))

        self.__server_cfg = cfg.get("server")
        self.__store_cfg  = cfg.get("store")

        self.STORE  = PireStore(self.__store_cfg)

    @SERVER.route("/pire/kv/create", methods=['POST'])
    async def create(self):
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = data.get("key")
            value = data.get("value")

            statemachine = self.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = self.STORE.database.create(key, value)
            grpc_visited = [__address_message(self.STORE.HOST, self.STORE.PORT)]

            if local_success: # Created successfully in the local
                grpc_write = __write_message(
                    key, value, 1, grpc_visited)

            else: # Failed to create
                grpc_write = __write_message(
                    key, value, 0, grpc_visited)

            success, _ = await self.STORE.cluster_handler.create_protocol(grpc_write)
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(status=status_code)
        

    @SERVER.route("/pire/kv/read", methods=['POST'])
    async def read(self):
        status_code = 400  # Initially, failure
        value       = None # Initially, no value is found
        data = await request.get_json()
        
        try: # Try to extract data and run protocol
            key   = data.get("key")

            statemachine = self.STORE.get_state_machine(key)
            statemachine.poll(Event.READ)
            statemachine.trigger(Event.READ)

            success, value, version = self.STORE.database.read(key)
            
            if success: # Read successfully from the local
                grpc_validate = __validate_message(key, value, version)
                val_value, val_version = await self.STORE.cluster_handler.validate_protocol(grpc_validate)
                
                if version < val_version: # Eventual-consistency !
                    self.STORE.database.validate(key, val_value, val_version)
                    value = val_value

            else: # Failed to read
                grpc_visited = [__address_message(self.STORE.HOST, self.STORE.PORT)]
                grpc_read = __read_message(key, grpc_visited)
                success, value, _ = await self.STORE.cluster_handler.read_protocol(grpc_read)
            
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(
            status=status_code,
            response=str(value))

    @SERVER.route("/pire/kv/update", methods=['POST'])
    async def update(self):
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = data.get("key")
            value = data.get("value")

            statemachine = self.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = self.STORE.database.update(key, value)
            grpc_visited = [__address_message(self.STORE.HOST, self.STORE.PORT)]

            if local_success: # Updated successfully in the local
                grpc_write = __write_message(
                    key, value, 1, grpc_visited)

            else: # Failed to update
                grpc_write = __write_message(
                    key, value, 0, grpc_visited)

            ack, _ = await self.STORE.cluster_handler.update_protocol(grpc_write)
            if ack: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(status=status_code)

    @SERVER.route("/pire/kv/delete", methods=['POST'])
    async def delete(self):
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = data.get("key")

            statemachine = self.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = self.STORE.database.delete(key)
            grpc_visited = [__address_message(self.STORE.HOST, self.STORE.PORT)]

            if local_success: # Updated successfully in the local
                grpc_write = __write_message(
                    key, None, 1, grpc_visited)

            else: # Failed to update
                grpc_write = __write_message(
                    key, None, 0, grpc_visited)

            success, _, _ = await self.STORE.cluster_handler.delete_protocol(grpc_write)
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(status=status_code)

    async def main(self) -> None:
        await self.STORE.run()
        await self.SERVER.run_task(
            debug = False,
            host  = "0.0.0.0",
            port  = self.__server_cfg.get("port"))
    