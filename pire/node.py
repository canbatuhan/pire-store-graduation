import yaml
from pire.server import PireServer
from pire.store  import PireStore

class PireNode:
    SERVER = None # quart app
    STORE  = None # pire-store

    def __init__(self, config_path:str=None) -> None:
        with open(config_path, "r") as file:
            cfg = dict(yaml.load(file))

        server_cfg = cfg.get("server")
        store_cfg  = cfg.get("store")

        self.SERVER = PireServer(server_cfg)
        self.STORE  = PireStore(store_cfg) 

    async def main(self) -> None:
        await self.STORE.run()
        await self.SERVER.run()
    