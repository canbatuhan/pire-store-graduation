import pickledb
import struct
from typing import Tuple

MODE = "<8sH" # Little endian (8-byte string | 2-byte uint)

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

    def create(self, key:str, value:str) -> bool:
        data = struct.pack(MODE, value.encode(), 0)
        self.__db.set(key, data)
        self.__size += 1
        return True

    def read(self, key:str) -> Tuple[bool,str,int]:
        entry = self.__db.get(key)
        if not entry: # Key does not exist
            return False, None, None
        value, version = struct.unpack(MODE, entry)
        value = value.decode().rstrip("\x00")
        return True, value, version

    def validate(self, key:str, value:str, version:int) -> Tuple[bool]:
        data = struct.pack(MODE, value.encode(), version)
        self.__db.set(key, data)
        return True

    def update(self, key:str, value:str) -> bool:
        entry = self.__db.get(key)
        if entry: # Key exists
            _, version = struct.unpack(MODE, entry)
            data = struct.pack(MODE, value.encode(), version+1)
            self.__db.set(key, data)
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