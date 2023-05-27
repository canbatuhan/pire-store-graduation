from enum import Enum

class Event(Enum):
    START = "START"
    END   = "END"
    READ  = "READ"
    WRITE = "WRITE"
    DONE  = "DONE"