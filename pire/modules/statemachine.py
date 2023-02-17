import time
from smpai.fsm import FiniteStateMachine

from pire.util.logger import Logger
from pire.util.constants import INITIAL_POLL_TIME, MAX_POLL_TIME
from pire.util.enums import Events
from pire.util.exceptions import PollingTimeoutException


class ReplicatedStateMachine:
    def __init__(self, client_str:str, config_path:str) -> None:
        self.__machine = FiniteStateMachine(config_path)
        self.__logger = Logger("Replicated-State-Machine", client_str)

    def start(self) -> None:
        self.__logger.info("Started.")
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
                self.__logger.failure("Event '{}' could not trigger the machine for {} seconds.".format(event.value, wait_time))
                raise PollingTimeoutException()

    def trigger(self, event:Events) -> bool:
        old_state = self.__machine.get_context().get_current_state().get_id()
        self.__machine.send_event(event.value)
        new_state = self.__machine.get_context().get_current_state().get_id()
        self.__logger.success("Event '{}' triggered the machine '{} -{}-> {}'.".format(
            event.value, old_state, new_state, event.value))
        return True

    def end(self) -> None:
        self.trigger(Events.END) # IDLE -> S_FINAL
        self.__logger.info("Ended.")