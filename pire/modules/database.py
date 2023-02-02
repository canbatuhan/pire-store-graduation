import pickledb
from typing import Tuple

from pire.util.constants import LOCAL_DB_PATH
from pire.util.logger import Logger

class LocalDatabase:
    def __init__(self, client_id:str) -> None:
        self.__db = pickledb.load(LOCAL_DB_PATH(client_id), True)
        self.__logger = Logger("Local-Database", client_id)

    def start(self) -> None:
        self.__logger.info("Started.")

    def create(self, key:object, value:object) -> bool:
        if self.__db.exists(key):
            old_value = self.__db.get(key)
            self.__db.set(key, value)
            self.__logger.warning("Key '{}' already exists in local database '{}:{}'.".format(key, key, old_value))
            self.__logger.warning("Pair '{}:{}' is updated as '{}:{}'".format(key, old_value, key, value))
            
        else: # Key does not exist
            self.__db.set(key, value)
            self.__logger.success("Pair '{}:{}' is created in local database.".format(key, value))

        return self.__db.exists(key)

    def read(self, key:object) -> Tuple[bool,object]:
        if self.__db.exists(key):
            value = self.__db.get(key)
            self.__logger.success("Key '{}' is found, Value '{}' is read.".format(key, value))
            return True, value

        else: # Key does not exist
            self.__logger.failure("Key '{}' is not found in local database.".format(key))
            return False, None

    def update(self, key:object, value:object) -> bool:
        if self.__db.exists(key):
            old_value = self.__db.get(key)
            self.__db.set(key, value)
            self.__logger.success("Pair '{}:{}' is updated as '{}:{}'".format(key, old_value, key, value))

        else: # Key does not exist
            self.__logger.failure("Key '{}' is not found in local database.".format(key))

        return self.__db.exists(key)

    def delete(self, key:object) -> bool:
        if self.__db.exists(key):
            old_value = self.__db.get(key)
            self.__db.rem(key)
            self.__logger.success("Pair '{}:{}' is removed from local database.".format(key, old_value))
            return True

        else: # Key does not exist
            self.__logger.failure("Key '{}' is not found in local database.".format(key))
            return False

    def seek(self, key:object) -> bool:
        if self.__db.exists(key):
            self.__logger.info("Seeking for Key '{}' in local database... it is found.".format(key))
            return True
        
        else: # Key does not exist
            self.__logger.info("Seeking for Key '{}' in local database... it is not found.".format(key))
            return False