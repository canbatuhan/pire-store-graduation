from quart import Quart, Response

class PireServer:
    app  = Quart(__name__)

    def __init__(self, config:dict) -> None:
        self.__host = "127.0.0.1"
        self.__port = config.get("port")

    @app.route("/pire/kv/create", methods=['POST'])
    async def create():
        print("Create protocol initiated")
        return Response(status=200)

    @app.route("/pire/kv/read", methods=['POST'])
    async def read():
        print("Read protocol initiated")
        return Response(status=200)

    @app.route("/pire/kv/update", methods=['POST'])
    async def update():
        print("Update protocol initiated")
        return Response(status=200)

    @app.route("/pire/kv/delete", methods=['POST'])
    async def delete():
        print("Delete protocol initiated")
        return Response(status=200)
    
    async def run(self):
        await self.app.run_task(
            debug = False,
            host  = self.__host,
            port  = self.__port)