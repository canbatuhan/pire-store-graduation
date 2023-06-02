import random
from typing import Tuple, List

MAX_HOPS = 9999999

class DestinationInfo:
    def __init__(self, owner:Tuple[str,int], nexts:List[Tuple[str,int]], hops:int) -> None:
        self.owner = owner
        self.nexts = nexts
        self.hops  = hops

class DiscoveryItem:
    def __init__(self) -> None:
        self.__destinations:List[DestinationInfo] = dict()
        self.__best_indexes:List[int] = list()
    
    def __exists(self, owner:Tuple[str,int]) -> bool:
        return self.get_item(owner)[1] != None
    
    def __set_best(self) -> None:
        min_hops = MAX_HOPS
        self.__best_indexes.clear()
        for each in self.__destinations:
            if each.hops != 0:
                if each.hops < min_hops:
                    self.__best_indexes.clear()
                    self.__best_indexes.append(each)
                elif each.hops == min_hops:
                    self.__best_indexes.append(each)

    def get_item(self, owner:Tuple[str,int]) -> Tuple[int,DestinationInfo]:
        for idx, each in enumerate(self.__destinations):
            if each.owner == owner:
                return idx, each
        return -1, None

    def get_destinations(self) -> List[DestinationInfo]:
        return self.__destinations
    
    def get_best_destination(self) -> DestinationInfo:
        return self.__destinations[random.choice(self.__best_indexes)]
    
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
            idx, dest_info = self.get_item(owner)

            # Better path, set
            if hops < dest_info.hops:
                self.__destinations[idx] = DestinationInfo(owner, [next], hops)

            elif hops == dest_info.hops and next not in dest_info.nexts:
                self.__destinations[idx].nexts.append(next)

            else: # Not better, do not set
                return False

        # Set best destinations
        self.__set_best()
        return True

    def delete_destination(self, owner) -> Tuple[bool,int]:
        # Destination exists, delete
        if self.__exists(owner):
            _, idx = self.get_item(owner)
            self.__destinations.pop(idx)
            self.__set_best()
            return True, len(self.__destinations)
        
        # Destination not exists, nothing to delete
        return False, len(self.__destinations)
