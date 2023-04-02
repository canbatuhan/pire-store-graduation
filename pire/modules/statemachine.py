import time
from smpai.fsm import FiniteStateMachine

from pire.util.constants import INITIAL_POLL_TIME, MAX_POLL_TIME
from pire.util.enums import Events
from pire.util.exceptions import PollingTimeoutException


class ReplicatedStateMachine:
    def __init__(self, config_path:str) -> None:
        self.__machine = FiniteStateMachine(config_path)

    def start(self) -> None:
        self.__machine.start() # Start machine -> INIT
        self.trigger(Events.START) # INIT -> IDLE

    def __check(self, event:Events) -> bool:
        return self.__machine.check_event(event.value)

    def poll(self, event:Events) -> None:
        wait_time = INITIAL_POLL_TIME # nanoseconds
        while not self.__check(event):
            time.sleep(wait_time)
            wait_time *= 2
            if wait_time >= MAX_POLL_TIME:
                raise PollingTimeoutException()

    def trigger(self, event:Events) -> bool:
        self.__machine.send_event(event.value)
        return True

    def end(self) -> None:
        self.trigger(Events.END) # IDLE -> S_FINAL