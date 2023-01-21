import json

from pire.util.constants import CLIENT_CONFIG_PATH
from pire.util.logger import Logger

class PireClient:
    """
        Class to represent a computing unit in the configured
        Raspberry Pi Cluster, `PiRe`.
    """

    def __init__(self, client_id:str) -> None:
        """
            Description:
                Constructs a `PireClient` object with given
                configurations.

            Arguments:
                - client_id : `str`, unique ID of the PiRe Client
        """
        file = open(CLIENT_CONFIG_PATH, 'r')
        config_paths = dict(json.load(file))

        self.__id = client_id
        self.__logger = Logger(self.__id)