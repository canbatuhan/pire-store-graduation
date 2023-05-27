import pickledb
from typing import Tuple

class LocalDatabase:
    def __init__(self, local_db_path:str) -> None:
        self.__db   = pickledb.load(local_db_path, False)
        self.__size = 0

    def start(self) -> None:
        pass

    def get_size(self) -> int:
        return self.__size

    def save(self) -> None:
        self.__db.dump()

    def create(self, key:object, value:object) -> bool:
        self.__db.set(key, value)
        self.__size += 1
        return True

    def read(self, key:object) -> Tuple[bool,object]:
        value = self.__db.get(key)
        if not value: # Key does not exist
            return False, None
        return True, value

    def update(self, key:object, value:object) -> bool:
        if self.__db.exists(key):
            self.__db.set(key, value)
            return True
        else: # Key does not exist
            return False

    def delete(self, key:object) -> bool:
        if self.__db.exists(key):
            self.__db.rem(key)
            self.__size -= 1
            return True
        else: # Key does not exist
            return False