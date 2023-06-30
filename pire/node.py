from quart import Quart, Response
from pire.storage import PireStorage

class PireNode:
    SERVER  = Quart(__name__)
    STORAGE = None # pire-store

    def __init__(self, node_config_path:str) -> None:
        self.STORAGE = PireStorage(node_config_path)

    @SERVER.route("/pire/kv/create", methods=['POST'])
    async def create():
        print("Create protocol initiated")
        return Response(status=200)

    @SERVER.route("/pire/kv/read", methods=['POST'])
    async def read():
        print("Read protocol initiated")
        return Response(status=200)

    @SERVER.route("/pire/kv/update", methods=['POST'])
    async def update():
        print("Update protocol initiated")
        return Response(status=200)

    @SERVER.route("/pire/kv/delete", methods=['POST'])
    async def delete():
        print("Delete protocol initiated")
        return Response(status=200)