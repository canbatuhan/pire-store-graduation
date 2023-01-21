from smpai.fsm import FiniteStateMachine

from pire.util.logger import Logger
from pire.util.enums import Events

class ReplicatedStateMachine:
    def __init__(self, config_path:str) -> None:
        self.__machine = FiniteStateMachine(config_path)
        self.__logger = Logger("Replicated-State-Machine")

    def start(self) -> None:
        self.__machine.start() # Start machine -> INIT
        self.trigger(Events.START) # INIT -> IDLE
        self.__logger.info("Started.")

    def check(self, event:Events) -> bool:
        if self.__machine.check_event(event.value):
            self.__logger.info("Event check... machine can be triggered by event '{}'.".format(event.value))
            return True

        else: # Can not be triggered
            self.__logger.info("Event check... machine can not be triggered by event '{}'.".format(event.value))
            return False

    def trigger(self, event:Events) -> bool:
        old_state = self.__machine.get_context().get_current_state().get_id()
        self.__machine.send_event(event.value)
        new_state = self.__machine.get_context().get_current_state().get_id()

        if old_state != new_state:
            self.__logger.success("Event '{}' triggered the machine '{} -{}-> {}'.".format(
                event.value, old_state, new_state, event.value))
            return True

        else: # Machine is not triggered
            self.__logger.failure("Event '{}' did not trigger the machine.".format(event.value))
            return False

    def end(self) -> None:
        self.trigger(Events.END) # IDLE -> S_FINAL
        self.__logger.info("Ended.")