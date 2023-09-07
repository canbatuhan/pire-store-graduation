import asyncio
from typing import List
import yaml
from quart import Quart, request, Response

from pire.store           import PireStore
from pire.modules.service import pirestore_pb2
from pire.util.event      import Event


def _address_message(host:str, port:int) -> pirestore_pb2.Address:
    return pirestore_pb2.Address(
        host = host,
        port = port
    )

def _write_message(key:str, value:str, replica:int, visited:List[pirestore_pb2.Address]) -> pirestore_pb2.WriteProtocolMessage:
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

def _read_message(key:str, visited:List[pirestore_pb2.Address]) -> pirestore_pb2.ReadProtocolMessage:
    return pirestore_pb2.ReadProtocolMessage(
        payload = pirestore_pb2.ReadPayload(
            key = key
        ),
        metadata = pirestore_pb2.ReadMetadata(
            visited = visited
        )
    )

def _validate_message(key:str, value:str, version:int) -> pirestore_pb2.ValidateProtocolMessage:
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
            cfg = dict(yaml.load(file, Loader=yaml.SafeLoader))

        self.__server_cfg = cfg.get("server")
        self.__store_cfg  = cfg.get("store")

        PireNode.STORE = PireStore(self.__store_cfg)

    @SERVER.route("/pire/kv/create", methods=['PUT'])
    async def create():
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = str(data.get("key"))
            value = str(data.get("value"))

            statemachine = PireNode.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = PireNode.STORE.database.create(key, value)
            grpc_visited = [_address_message(PireNode.STORE.HOST, PireNode.STORE.PORT)]

            if local_success: # Created successfully in the local
                grpc_write = _write_message(
                    key, value, 1, grpc_visited)

            else: # Failed to create
                grpc_write = _write_message(
                    key, value, 0, grpc_visited)

            success, _ = await PireNode.STORE.cluster_handler.create_protocol(grpc_write)
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except Exception as exception: # Failed to read request or state machine polling exception
            print(exception.with_traceback(None))
            pass

        return Response(status=status_code)
        

    @SERVER.route("/pire/kv/read", methods=['PUT'])
    async def read():
        status_code = 400  # Initially, failure
        value       = None # Initially, no value is found
        data = await request.get_json()
        
        try: # Try to extract data and run protocol
            key   = str(data.get("key"))

            statemachine = PireNode.STORE.get_state_machine(key)
            statemachine.poll(Event.READ)
            statemachine.trigger(Event.READ)

            success, value, version = PireNode.STORE.database.read(key)
            
            if success: # Read successfully from the local
                grpc_validate = _validate_message(key, value, version)
                val_value, val_version = await PireNode.STORE.cluster_handler.validate_protocol(grpc_validate)
                
                if version < val_version: # Eventual-consistency !
                    PireNode.STORE.database.validate(key, val_value, val_version)
                    value = val_value

            else: # Failed to read
                grpc_visited = [_address_message(PireNode.STORE.HOST, PireNode.STORE.PORT)]
                grpc_read = _read_message(key, grpc_visited)
                success, value, _ = await PireNode.STORE.cluster_handler.read_protocol(grpc_read)
            
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(
            status=status_code,
            response=str(value))

    @SERVER.route("/pire/kv/update", methods=['PUT'])
    async def update():
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = str(data.get("key"))
            value = str(data.get("value"))

            statemachine = PireNode.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = PireNode.STORE.database.update(key, value)
            grpc_visited = [_address_message(PireNode.STORE.HOST, PireNode.STORE.PORT)]

            if local_success: # Updated locally
                status_code = 200

            else: # Failed to update
                grpc_write = _write_message(
                    key, value, 0, grpc_visited)
                ack, _ = await PireNode.STORE.cluster_handler.update_protocol(grpc_write)
            
                if ack: # Success criteria is met
                    status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(status=status_code)

    @SERVER.route("/pire/kv/delete", methods=['PUT'])
    async def delete():
        status_code = 400 # Initially, failure
        data = await request.get_json()

        try: # Try to extract data and run protocol
            key   = str(data.get("key"))

            statemachine = PireNode.STORE.get_state_machine(key)
            statemachine.poll(Event.WRITE)
            statemachine.trigger(Event.WRITE)

            local_success = PireNode.STORE.database.delete(key)
            grpc_visited = [_address_message(PireNode.STORE.HOST, PireNode.STORE.PORT)]

            if local_success: # Updated successfully in the local
                grpc_write = _write_message(
                    key, None, 1, grpc_visited)

            else: # Failed to update
                grpc_write = _write_message(
                    key, None, 0, grpc_visited)

            success, _, _ = await PireNode.STORE.cluster_handler.delete_protocol(grpc_write)
            if success: # Success criteria is met
                status_code = 200

            statemachine.trigger(Event.DONE)

        except: # Failed to read request or state machine polling exception
            pass

        return Response(status=status_code)

    async def main(self) -> None:
        await asyncio.gather(
            PireNode.STORE.run(),
            PireNode.SERVER.run_task(
                debug = False,
                host  = "0.0.0.0",
                port  = self.__server_cfg.get("port"))
        )
    