from datetime import datetime

from pire.util.constants import LOG_FILE_PATH


class Logger:
    def __init__(self, component:str, client_id:str) -> None:
        self.__file_path = LOG_FILE_PATH(client_id)
        self.__component = component

    def __timestamp(self) -> str:
        return datetime.now().strftime("%y/%m/%d %H:%M:%S.%f")

    def info(self, msg:str) -> None:
        file = open(self.__file_path, 'a')
        file.write("[{}] {} - INFO : {}\n".format(self.__component, self.__timestamp(), msg))
        file.close()

    def success(self, msg:str) -> None:
        file = open(self.__file_path, 'a')
        file.write("[{}] {} - SUCCESS : {}\n".format(self.__component, self.__timestamp(), msg))
        file.close()

    def warning(self, msg:str) -> None:
        file = open(self.__file_path, 'a')
        file.write("[{}] {} - WARNING : {}\n".format(self.__component, self.__timestamp(), msg))
        file.close()

    def failure(self, msg:str) -> None:
        file = open(self.__file_path, 'a')
        file.write("[{}] {} - FAIL : {}\n".format(self.__component, self.__timestamp(), msg))
        file.close()