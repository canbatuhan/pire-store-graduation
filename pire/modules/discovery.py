import random
from typing import Tuple, List

MAX_HOPS = 9999999

class DestinationInfo:
    def __init__(self, owner, next:Tuple[str,int], hops:int) -> None:
        self.owner = owner
        self.next  = next
        self.hops   = hops

class DiscoveryItem:
    def __init__(self) -> None:
        self.__destinations:List[DestinationInfo] = dict()
        self.__best_indexes:List[int] = list()
    
    def __get_item(self, owner:Tuple[str,int]) -> Tuple[int,DestinationInfo]:
        for idx, each in enumerate(self.__destinations):
            if each.owner == owner:
                return idx, owner
        return -1, None
    
    def __exists(self, owner:Tuple[str,int]) -> bool:
        return self.__get_item(owner)[1] != None
    
    def __set_best(self) -> None:
        min_hops = MAX_HOPS
        self.__best_indexes.clear()
        for each in self.__destinations:
            if each.hops < min_hops:
                self.__best_indexes.clear()
                self.__best_indexes.append(each)
            elif each.hops == min_hops:
                self.__best_indexes.append(each)

    def get_destinations(self) -> List[DestinationInfo]:
        return self.__destinations
    
    def get_best_next(self) -> Tuple[str,int]:
        return self.__destinations[random.choice(self.__best_indexes)].next
    
    def set_destination(self, owner:Tuple[str,int], next:Tuple[str,int], hops:int) -> bool:

        # First entry, set
        if len(self.__best_indexes) == 0:
            self.__best_indexes.append(0)
            self.__destinations.append(DestinationInfo(owner, next, hops))

        # Destination not exists, set
        elif not self.__exists(owner):
            self.__destinations.append(
                DestinationInfo(owner, next, hops))

        else: # Destination exists, might not set
            idx, dest_info = self.__get_item(owner)

            # Better path, set
            if hops < dest_info.hops:
                self.__destinations[idx] = DestinationInfo(owner, next, hops)

            else: # Not better, do not set
                return False

        # Set best destinations
        self.__set_best()
        return True

    def delete_destination(self, owner) -> Tuple[bool,int]:
        # Destination exists, delete
        if self.__exists(owner):
            _, idx = self.__get_item(owner)
            self.__destinations.pop(idx)
            self.__set_best()
            return True, len(self.__destinations)
        
        # Destination not exists, nothing to delete
        return False, len(self.__destinations)
