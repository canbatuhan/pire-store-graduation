import pickledb
import struct
from typing import Tuple

MODE = "<sH" # Little endian (bytes-(high)word)

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

    def create(self, key:bytes, value:bytes) -> bool:
        self.__db.set(key, struct.pack(MODE, value, 0))
        self.__size += 1
        return True

    def read(self, key:bytes) -> Tuple[bool,bytes,int]:
        entry = self.__db.get(key)
        if not entry: # Key does not exist
            return False, None, None
        value, version = struct.unpack(MODE, entry)
        return True, value, version

    def validate(self, key:bytes, value:bytes, version:int) -> Tuple[bool]:
        self.__db.set(key, struct.pack(MODE, value, version))
        return True

    def update(self, key:bytes, value:bytes) -> bool:
        entry = self.__db.get(key)
        if entry: # Key exists
            value, version = struct.unpack(MODE, entry)
            self.__db.set(key, struct.pack(MODE, value, version+1))
            return True
        else: # Key does not exist
            return False

    def delete(self, key:bytes) -> bool:
        if self.__db.exists(key):
            self.__db.rem(key)
            self.__size -= 1
            return True
        else: # Key does not exist
            return False