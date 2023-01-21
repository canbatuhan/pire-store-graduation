from enum import Enum

class Events(Enum):
    START = "START"
    END = "END"
    CREATE = "CREATE"
    READ = "READ"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    DONE = "DONE"
