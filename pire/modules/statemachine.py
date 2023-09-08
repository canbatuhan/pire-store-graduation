from io import TextIOWrapper
import time
from smpai.fsm import FiniteStateMachine

from pire.util.event     import Event
from pire.util.exception import PollingTimeoutException

SM_CONFIG_PATH = "pire/resource/statemachine.json"

class ReplicatedStateMachine:
    MIN_POLL_TIME    = float()
    MAX_POLL_TIME    = float()

    def __init__(self, min_poll_time:float, max_poll_time:float) -> None:
        self.__machine     = FiniteStateMachine(SM_CONFIG_PATH)
        self.MIN_POLL_TIME = min_poll_time
        self.MAX_POLL_TIME = max_poll_time

    def start(self) -> None:
        self.__machine.start() # Start machine -> INIT
        self.trigger(Event.START) # INIT -> IDLE

    def __check(self, event:Event) -> bool:
        return self.__machine.check_event(event.value)

    def poll(self, event:Event) -> None:
        wait_time = self.MIN_POLL_TIME
        while not self.__check(event):
            time.sleep(wait_time)
            wait_time *= 2
            if wait_time >= self.MAX_POLL_TIME:
                raise PollingTimeoutException()

    def trigger(self, event:Event) -> bool:
        self.__machine.send_event(event.value)
        return True

    def end(self) -> None:
        self.trigger(Event.END) # IDLE -> S_FINAL