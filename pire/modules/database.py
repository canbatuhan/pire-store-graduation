import pickledb
from typing import Tuple

from pire.util.constants import LOCAL_DB_PATH

class LocalDatabase:
    def __init__(self) -> None:
        self.__db = pickledb.load(LOCAL_DB_PATH, False)

    def start(self) -> None:
        pass

    def get_size(self) -> int:
        return len(self.__db.getall())

    def save(self) -> None:
        self.__db.dump()

    def create(self, key:object, value:object) -> bool:
        if self.__db.exists(key):
            self.__db.set(key, value)
            
        else: # Key does not exist
            self.__db.set(key, value)

        return self.__db.exists(key)

    def read(self, key:object) -> Tuple[bool,object]:
        if self.__db.exists(key):
            value = self.__db.get(key)
            return True, value

        else: # Key does not exist
            return False, None

    def update(self, key:object, value:object) -> bool:
        if self.__db.exists(key):
            self.__db.set(key, value)
        return self.__db.exists(key)

    def delete(self, key:object) -> bool:
        if self.__db.exists(key):
            self.__db.rem(key)
            return True

        else: # Key does not exist
            return False

    def seek(self, key:object) -> bool:
        if self.__db.exists(key):
            return True
        
        else: # Key does not exist
            return False