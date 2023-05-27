from enum import Enum

class Events(Enum):
    START = "START"
    END = "END"
    CREATE = "CREATE"
    CREATE_REDIR = "CREATE_REDIR"
    READ = "READ"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    DONE = "DONE"
